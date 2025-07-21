package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static String writeSummaryJsonToFile(SummaryPayload payload) {
        if (payload == null) {
            logger.error("SummaryPayload is null. Cannot write summary.json.");
            throw new IllegalArgumentException("SummaryPayload cannot be null");
        }

        try {
            String batchId = Optional.ofNullable(payload.getBatchID()).orElse("unknown");
            String fileName = "summary_" + batchId + ".json";

            Path tempDir = Files.createTempDirectory("summaryFiles");
            Path summaryFilePath = tempDir.resolve(fileName);

            File summaryFile = summaryFilePath.toFile();
            if (summaryFile.exists()) {
                Files.delete(summaryFilePath);
                logger.warn("Existing summary file deleted: {}", summaryFilePath);
            }

            objectMapper.writeValue(summaryFile, payload);
            logger.info("✅ Summary JSON written at: {}", summaryFilePath);

            return summaryFilePath.toAbsolutePath().toString();

        } catch (Exception e) {
            logger.error("❌ Failed to write summary.json", e);
            throw new RuntimeException("Failed to write summary JSON", e);
        }
    }

    public static SummaryPayload buildPayload(
            KafkaMessage kafkaMessage,
            List<SummaryProcessedFile> processedList,
            String summaryBlobUrl,
            String fileName,
            String batchId,
            String timestamp
    ) {
        SummaryPayload payload = new SummaryPayload();
        payload.setBatchID(batchId);
        payload.setFileName(fileName);
        payload.setTimestamp(timestamp);
        payload.setSummaryFileURL(summaryBlobUrl);

        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
        payload.setProcessedFileList(processedFileEntries);

        int totalFileUrls = processedFileEntries.stream()
                .mapToInt(entry -> {
                    int count = 0;
                    if (entry.getPdfEmailFileUrl() != null && !entry.getPdfEmailFileUrl().isBlank()) count++;
                    if (entry.getPdfArchiveFileUrl() != null && !entry.getPdfArchiveFileUrl().isBlank()) count++;
                    if (entry.getPdfMobstatFileUrl() != null && !entry.getPdfMobstatFileUrl().isBlank()) count++;
                    if (entry.getPrintFileUrl() != null && !entry.getPrintFileUrl().isBlank()) count++;
                    return count;
                })
                .sum();

        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
        payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
        payloadInfo.setEventID(kafkaMessage.getEventID());
        payloadInfo.setEventType(kafkaMessage.getEventType());
        payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
        payloadInfo.setFileCount(totalFileUrls);
        payload.setPayload(payloadInfo);

        Metadata metadata = new Metadata();
        metadata.setTotalCustomersProcessed((int) processedFileEntries.stream()
                .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
                .distinct()
                .count());

        long total = processedFileEntries.size();
        long success = processedFileEntries.stream()
                .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverAllStatusCode()))
                .count();
        long failed = processedFileEntries.stream()
                .filter(entry -> "FAILED".equalsIgnoreCase(entry.getOverAllStatusCode()))
                .count();

        String overallStatus;
        if (success == total) {
            overallStatus = "SUCCESS";
        } else if (failed == total) {
            overallStatus = "FAILED";
        } else {
            overallStatus = "PARTIAL";
        }

        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        return payload;
    }

    private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
        List<ProcessedFileEntry> finalList = new ArrayList<>();

        Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
                .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
                .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

        for (Map.Entry<String, List<SummaryProcessedFile>> group : grouped.entrySet()) {
            String[] parts = group.getKey().split("::");
            String customerId = parts[0];
            String accountNumber = parts[1];

            Map<String, SummaryProcessedFile> methodMap = new HashMap<>();
            Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();

            for (SummaryProcessedFile file : group.getValue()) {
                String method = file.getOutputMethod();
                if (method == null) continue;

                switch (method.toUpperCase()) {
                    case "EMAIL", "MOBSTAT", "PRINT" -> methodMap.put(method.toUpperCase(), file);
                    case "ARCHIVE" -> {
                        String linked = file.getLinkedDeliveryType();
                        if (linked != null) {
                            archiveMap.put(linked.toUpperCase(), file);
                        }
                    }
                }
            }

            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(customerId);
            entry.setAccountNumber(accountNumber);

            List<String> statuses = new ArrayList<>();
            boolean hasSuccess = false;

            for (String type : List.of("EMAIL", "MOBSTAT", "PRINT")) {
                SummaryProcessedFile delivery = methodMap.get(type);
                SummaryProcessedFile archive = archiveMap.get(type);

                String deliveryStatus = null, archiveStatus = null;

                if (delivery != null) {
                    String url = delivery.getBlobURL();
                    deliveryStatus = delivery.getStatus();
                    String reason = delivery.getStatusDescription();

                    switch (type) {
                        case "EMAIL" -> {
                            entry.setPdfEmailFileUrl(url);
                            entry.setPdfEmailFileUrlStatus(deliveryStatus);
                            if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                        }
                        case "MOBSTAT" -> {
                            entry.setPdfMobstatFileUrl(url);
                            entry.setPdfMobstatFileUrlStatus(deliveryStatus);
                            if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                        }
                        case "PRINT" -> {
                            entry.setPrintFileUrl(url);
                            entry.setPrintFileUrlStatus(deliveryStatus);
                            if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                        }
                    }

                    if ("SUCCESS".equalsIgnoreCase(deliveryStatus)) hasSuccess = true;
                }

                if (archive != null) {
                    archiveStatus = archive.getStatus();
                    String aUrl = archive.getBlobURL();

                    entry.setPdfArchiveFileUrl(aUrl); // shared field
                    entry.setPdfArchiveFileUrlStatus(archiveStatus);

                    if ("FAILED".equalsIgnoreCase(archiveStatus) && entry.getReason() == null) {
                        entry.setReason(archive.getStatusDescription());
                    }

                    if ("SUCCESS".equalsIgnoreCase(archiveStatus)) hasSuccess = true;
                }

                // Record method-level status only if at least one file is present
                if (deliveryStatus != null || archiveStatus != null) {
                    if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                        statuses.add("SUCCESS");
                    } else if ("FAILED".equalsIgnoreCase(deliveryStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
                        statuses.add("FAILED");
                    } else {
                        statuses.add("PARTIAL");
                    }
                }
            }

            // ✅ Skip if no file for this customer is SUCCESS
            if (!hasSuccess) continue;

            // ✅ Determine overall status
            if (statuses.stream().allMatch(s -> "SUCCESS".equals(s))) {
                entry.setOverAllStatusCode("SUCCESS");
            } else if (statuses.stream().allMatch(s -> "FAILED".equals(s))) {
                entry.setOverAllStatusCode("FAILED");
            } else {
                entry.setOverAllStatusCode("PARTIAL");
            }

            finalList.add(entry);
        }

        return finalList;
    }
}


===========

package com.deloitte.drt.service;

import com.deloitte.drt.model.ErrorReportEntry;
import com.deloitte.drt.model.ProcessedFileEntry;
import com.deloitte.drt.model.SummaryProcessedFile;
import com.deloitte.drt.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
public class SummaryJsonWriter {
    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);

    public List<SummaryProcessedFile> parseSTDXml(File stdXmlFile) {
        List<SummaryProcessedFile> entries = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(stdXmlFile));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("<file name=")) {
                    String name = extractAttribute(line, "name");
                    String queueName = extractAttribute(line, "name", "queue");
                    if (name != null && queueName != null) {
                        String[] parts = name.split("/");
                        String fileName = parts[parts.length - 1];

                        String[] nameParts = fileName.split("_");
                        String cis = nameParts.length > 0 ? nameParts[0] : "";
                        String account = nameParts.length > 1 ? nameParts[1] : "";

                        SummaryProcessedFile entry = new SummaryProcessedFile();
                        entry.setQueueName(queueName);
                        entry.setCisNumber(cis);
                        entry.setAccountNumber(account);
                        entry.setStatus("success");
                        entry.setFileUrl(name);
                        entries.add(entry);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error parsing STD XML file", e);
        }
        return entries;
    }

    private String extractAttribute(String line, String attribute) {
        Pattern pattern = Pattern.compile(attribute + "=\"(.*?)\"");
        Matcher matcher = pattern.matcher(line);
        return matcher.find() ? matcher.group(1) : null;
    }

    private String extractAttribute(String line, String attribute, String tagName) {
        if (!line.contains("<" + tagName)) return null;
        return extractAttribute(line, attribute);
    }

    public List<ErrorReportEntry> parseErrorReport(Path errorReportPath) {
        List<ErrorReportEntry> errorEntries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(errorReportPath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 5) {
                    ErrorReportEntry entry = new ErrorReportEntry();
                    entry.setCisNumber(parts[0].trim());
                    entry.setAccountNumber(parts[1].trim());
                    entry.setQueueName(parts[3].trim().toLowerCase());
                    entry.setErrorType(parts[2].trim());
                    entry.setErrorStatus(parts[4].trim());
                    errorEntries.add(entry);
                }
            }
        } catch (Exception e) {
            logger.error("Error parsing error report file", e);
        }
        return errorEntries;
    }

    public List<ProcessedFileEntry> buildDetailedProcessedFiles(String cisNumber, String accountNumber, List<String> requestedQueues, List<SummaryProcessedFile> foundFiles, List<ErrorReportEntry> errorEntries) {
        List<ProcessedFileEntry> processedFileEntries = new ArrayList<>();

        for (String queue : requestedQueues) {
            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCisNumber(cisNumber);
            entry.setAccountNumber(accountNumber);
            entry.setType(queue);

            Optional<SummaryProcessedFile> found = foundFiles.stream()
                    .filter(f -> f.getQueueName().equalsIgnoreCase(queue) &&
                            StringUtils.equalsIgnoreCase(f.getCisNumber(), cisNumber) &&
                            StringUtils.equalsIgnoreCase(f.getAccountNumber(), accountNumber))
                    .findFirst();

            if (found.isPresent()) {
                entry.setStatus(Constants.STATUS_SUCCESS);
                entry.setUrl(found.get().getFileUrl());
            } else {
                Optional<ErrorReportEntry> error = errorEntries.stream()
                        .filter(e -> StringUtils.equalsIgnoreCase(e.getQueueName(), queue) &&
                                StringUtils.equalsIgnoreCase(e.getCisNumber(), cisNumber) &&
                                StringUtils.equalsIgnoreCase(e.getAccountNumber(), accountNumber))
                        .findFirst();

                if (error.isPresent()) {
                    entry.setStatus(Constants.STATUS_FAILED);
                } else {
                    entry.setStatus(Constants.STATUS_NOT_FOUND);
                }
            }

            processedFileEntries.add(entry);
        }

        return processedFileEntries;
    }
}
