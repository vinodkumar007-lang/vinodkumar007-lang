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
import java.util.stream.Stream;

import static io.micrometer.common.util.StringUtils.isBlank;

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
            String timestamp,
            Map<String, Map<String, String>> errorMap
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

        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap);
        payload.setProcessedFileList(processedFileEntries);

        int totalFileUrls = (int) processedFileEntries.stream()
                .flatMap(entry -> Stream.of(
                        new AbstractMap.SimpleEntry<>(entry.getEmailBlobUrl(), entry.getEmailStatus()),
                        new AbstractMap.SimpleEntry<>(entry.getPrintBlobUrl(), entry.getPrintStatus()),
                        new AbstractMap.SimpleEntry<>(entry.getMobstatBlobUrl(), entry.getMobstatStatus()),
                        new AbstractMap.SimpleEntry<>(entry.getArchiveBlobUrl(), entry.getArchiveStatus())
                ))
                .filter(e -> e.getKey() != null && !e.getKey().trim().isEmpty()
                        && "SUCCESS".equalsIgnoreCase(e.getValue()))
                .count();

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
        /*Set<String> statuses = processedFileEntries.stream()
                .map(ProcessedFileEntry::getOverallStatus)
                .filter(Objects::nonNull)
                .map(String::toUpperCase)
                .collect(Collectors.toSet());*/

        /*String overallStatus = "FAILED";
        Iterator<String> statusIterator = statuses.iterator();

        while(statusIterator.hasNext()){
           String status = statusIterator.next();
            if(status.equals("PARTIAL")){
                overallStatus = "PARTIAL";
                break;
            }if(status.equals("SUCCESS")){
                overallStatus = "SUCCESS";
            }
        }*/

       /* String overallStatus;
        if (statuses.size() == 1) {
            overallStatus = statuses.iterator().next(); // only one unique status
        } else if (statuses.contains("SUCCESS") && statuses.contains("FAILED")) {
            overallStatus = "PARTIAL";
        } else if (statuses.contains("PARTIAL") || statuses.size() > 1) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "FAILED"; // fallback
        }*/

        // Collect all overallStatuses for individual records
        Set<String> statuses = processedFileEntries.stream()
                .map(ProcessedFileEntry::getOverallStatus)
                .collect(Collectors.toSet());

// Final overall status for the payload
        String overallStatus;
        if (statuses.size() == 1) {
            overallStatus = statuses.iterator().next();
        } else if (statuses.contains("SUCCESS") && statuses.contains("FAILED")) {
            overallStatus = "PARTIAL";
        } else if (statuses.contains("PARTIAL") || statuses.size() > 1) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "FAILED";
        }

        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        return payload;
    }

    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap) {

        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();
            ProcessedFileEntry entry = grouped.computeIfAbsent(key, k -> {
                ProcessedFileEntry newEntry = new ProcessedFileEntry();
                newEntry.setCustomerId(file.getCustomerId());
                newEntry.setAccountNumber(file.getAccountNumber());
                return newEntry;
            });

            String outputType = file.getOutputType();
            String blobUrl = file.getBlobUrl();
            Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

            String status;
            if (isNonEmpty(blobUrl)) {
                status = "SUCCESS";
            } else if ("FAILED".equalsIgnoreCase(errors.getOrDefault(outputType, ""))) {
                status = "FAILED";
            } else {
                status = "NOT_FOUND";
            }

            switch (outputType) {
                case "EMAIL" -> {
                    entry.setEmailBlobUrl(blobUrl);
                    entry.setEmailStatus(status);
                }
                case "PRINT" -> {
                    entry.setPrintFileUrl(blobUrl);
                    entry.setPrintStatus(status);
                }
                case "MOBSTAT" -> {
                    entry.setMobstatBlobUrl(blobUrl);
                    entry.setMobstatStatus(status);
                }
                case "ARCHIVE" -> {
                    entry.setArchiveBlobUrl(blobUrl);
                    entry.setArchiveStatus(status);
                }
            }
        }

        // ✅ Enhanced overallStatus logic with combo handling
        for (ProcessedFileEntry entry : grouped.values()) {
            String email = entry.getEmailStatus();
            String print = entry.getPrintStatus();
            String mobstat = entry.getMobstatStatus();
            String archive = entry.getArchiveStatus();

            boolean isEmailSuccess = "SUCCESS".equals(email);
            boolean isPrintSuccess = "SUCCESS".equals(print);
            boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
            boolean isArchiveSuccess = "SUCCESS".equals(archive);

            boolean isEmailMissingOrFailed = email == null || "FAILED".equals(email) || "NOT_FOUND".equals(email);
            boolean isPrintMissingOrFailed = print == null || "FAILED".equals(print) || "NOT_FOUND".equals(print);
            boolean isMobstatMissingOrFailed = mobstat == null || "FAILED".equals(mobstat) || "NOT_FOUND".equals(mobstat);

            if (isEmailSuccess && isArchiveSuccess) {
                entry.setOverallStatus("SUCCESS");
            } else if (isMobstatSuccess && isArchiveSuccess && isEmailMissingOrFailed && isPrintMissingOrFailed) {
                entry.setOverallStatus("SUCCESS");
            } else if (isPrintSuccess && isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed) {
                entry.setOverallStatus("SUCCESS");
            } else if (isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed && isPrintMissingOrFailed) {
                entry.setOverallStatus("PARTIAL");
            } else if (isArchiveSuccess) {
                entry.setOverallStatus("PARTIAL");
            } else {
                entry.setOverallStatus("FAILED");
            }
        }
        return new ArrayList<>(grouped.values());
    }

    private static boolean isNonEmpty(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
