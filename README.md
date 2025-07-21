package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.codec.net.URLCodec.decodeUrl;

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

        // HEADER
        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        // ✅ Final Processed Entries
        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
        payload.setProcessedFileList(processedFileEntries);

        // ✅ Dynamically count total non-null URLs (files actually added to summary)
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

        // PAYLOAD BLOCK
        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
        payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
        payloadInfo.setEventID(kafkaMessage.getEventID());
        payloadInfo.setEventType(kafkaMessage.getEventType());
        payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
        payloadInfo.setFileCount(totalFileUrls);
        payload.setPayload(payloadInfo);

        // METADATA
        Metadata metadata = new Metadata();
        metadata.setTotalCustomersProcessed((int) processedFileEntries.stream()
                .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
                .distinct()
                .count());

        // ✅ Determine overall status (Success / Partial / Failure)
        long total = processedFileEntries.size();
        long success = processedFileEntries.stream()
                .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverAllStatusCode()))
                .count();
        long failure = processedFileEntries.stream()
                .filter(entry -> "FAILURE".equalsIgnoreCase(entry.getOverAllStatusCode()))
                .count();

        String overallStatus;
        if (success == total) {
            overallStatus = "SUCCESS";
        } else if (failure == total) {
            overallStatus = "FAILURE";
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

            // Create maps by output method
            Map<String, SummaryProcessedFile> methodMap = new HashMap<>();
            Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();

            for (SummaryProcessedFile file : group.getValue()) {
                String method = file.getOutputMethod();
                if (method == null) continue;

                switch (method) {
                    case "EMAIL", "MOBSTAT", "PRINT" -> methodMap.put(method, file);
                    case "ARCHIVE" -> {
                        String linked = file.getLinkedDeliveryType(); // email/mobstat/print
                        if (linked != null) {
                            archiveMap.put(linked.toUpperCase(), file); // key as EMAIL, etc.
                        }
                    }
                }
            }

            // Now build combinations
            for (String type : List.of("EMAIL", "MOBSTAT", "PRINT")) {
                SummaryProcessedFile delivery = methodMap.get(type);
                SummaryProcessedFile archive = archiveMap.get(type);

                ProcessedFileEntry entry = new ProcessedFileEntry();
                entry.setCustomerId(customerId);
                entry.setAccountNumber(accountNumber);

                // Set delivery file info
                if (delivery != null) {
                    String url = delivery.getBlobURL();
                    String status = delivery.getStatus();
                    String reason = delivery.getStatusDescription();

                    switch (type) {
                        case "EMAIL" -> {
                            entry.setPdfEmailFileUrl(url);
                            entry.setPdfEmailFileUrlStatus(status);
                            if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                        }
                        case "MOBSTAT" -> {
                            entry.setPdfMobstatFileUrl(url);
                            entry.setPdfMobstatFileUrlStatus(status);
                            if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                        }
                        case "PRINT" -> {
                            entry.setPrintFileUrl(url);
                            entry.setPrintFileUrlStatus(status);
                            if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                        }
                    }
                }

                // Set archive info for that type
                if (archive != null) {
                    entry.setPdfArchiveFileUrl(archive.getBlobURL());
                    entry.setPdfArchiveFileUrlStatus(archive.getStatus());
                    if ("FAILED".equalsIgnoreCase(archive.getStatus())) {
                        entry.setReason(archive.getStatusDescription());
                    }
                }

                // Compute overall status for the pair
                String deliveryStatus = delivery != null ? delivery.getStatus() : "FAILED";
                String archiveStatus = archive != null ? archive.getStatus() : "FAILED";

                if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                    entry.setOverAllStatusCode("SUCCESS");
                } else if ("FAILED".equalsIgnoreCase(deliveryStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
                    entry.setOverAllStatusCode("FAILED");
                } else {
                    entry.setOverAllStatusCode("PARTIAL");
                }

                finalList.add(entry);
            }
        }

        return finalList;
    }

    private static String buildMobstatTrigger(List<SummaryProcessedFile> list) {
        return list.stream()
                .map(SummaryProcessedFile::getBlobURL)
                .filter(url -> url != null && url.contains("/DropData.trigger"))
                .findFirst()
                .orElse(null);
    }
}
