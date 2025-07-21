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
import java.util.function.Function;
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
            String timestamp,Map<String, Map<String, String>> errorMap
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

        for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
            String[] parts = entry.getKey().split("::");
            String customerId = parts[0];
            String accountNumber = parts[1];

            ProcessedFileEntry result = new ProcessedFileEntry();
            result.setCustomerId(customerId);
            result.setAccountNumber(accountNumber);

            boolean hasFailed = false;
            boolean hasNotFound = false;

            for (SummaryProcessedFile file : entry.getValue()) {
                String status = file.getStatus();
                String blobUrl = file.getBlobURL();
                String method = file.getOutputType();

                if ("FAILED".equalsIgnoreCase(status)) hasFailed = true;
                if ("NOT-FOUND".equalsIgnoreCase(status)) hasNotFound = true;

                switch (method.toUpperCase()) {
                    case "EMAIL":
                        result.setPdfEmailFileUrl(blobUrl);
                        result.setPdfEmailFileUrlStatus(status);
                        break;
                    case "ARCHIVE":
                        result.setPdfArchiveFileUrl(blobUrl);
                        result.setPdfArchiveFileUrlStatus(status);
                        break;
                    case "PRINT":
                        result.setPrintFileUrl(blobUrl);
                        result.setPrintFileUrlStatus(status);
                        break;
                    case "MOBSTAT":
                        result.setPdfMobstatFileUrl(blobUrl);
                        result.setPdfMobstatFileUrlStatus(status);
                        break;
                }
            }

            // Set overall status
            if (hasFailed) {
                result.setOverAllStatusCode("FAILED");
            } else if (hasNotFound) {
                result.setOverAllStatusCode("PARTIAL");
            } else {
                result.setOverAllStatusCode("SUCCESS");
            }

            finalList.add(result);
        }

        return finalList;
    }

    private static String determineOverallStatus(ProcessedFileEntry pf, Map<String, String> methodErrors) {
        List<String> methods = new ArrayList<>();

        if (pf.getPdfEmailFileUrlStatus() != null) {
            methods.add("EMAIL");
            methods.add("ARCHIVE");
        } else if (pf.getPdfMobstatFileUrlStatus() != null) {
            methods.add("MOBSTAT");
            methods.add("ARCHIVE");
        } else if (pf.getPrintFileUrlStatus() != null) {
            methods.add("PRINT");
            methods.add("ARCHIVE");
        }

        int successCount = 0;
        int failedInErrorMapCount = 0;
        int failedNotInErrorMapCount = 0;

        for (String method : methods) {
            String status = getStatusByMethod(pf, method);
            if ("SUCCESS".equalsIgnoreCase(status)) {
                successCount++;
            } else if ("FAILED".equalsIgnoreCase(status)) {
                if (methodErrors.containsKey(method)) {
                    failedInErrorMapCount++;
                } else {
                    failedNotInErrorMapCount++;
                }
            }
        }

        if (successCount == methods.size()) {
            return "SUCCESS";
        } else if (failedInErrorMapCount > 0) {
            return "FAILED";
        } else {
            return "PARTIAL";
        }
    }
    private static String getStatusByMethod(ProcessedFileEntry pf, String method) {
        switch (method.toUpperCase()) {
            case "EMAIL":
                return pf.getPdfEmailFileUrlStatus();
            case "ARCHIVE":
                return pf.getPdfArchiveFileUrlStatus();
            case "MOBSTAT":
                return pf.getPdfMobstatFileUrlStatus();
            case "PRINT":
                return pf.getPrintFileUrlStatus();
            default:
                return null;
        }
    }
}
