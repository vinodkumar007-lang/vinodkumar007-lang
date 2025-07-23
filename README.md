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
        long success = processedFileEntries.stream()
                .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverallStatus()))
                .count();
        long failed = processedFileEntries.stream()
                .filter(entry -> "FAILED".equalsIgnoreCase(entry.getOverallStatus()))
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

    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap) {

        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();
            ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            String outputMethod = file.getOutputMethod();
            String status = file.getStatus();
            String blobUrl = file.getBlobUrl();

            switch (outputMethod.toUpperCase()) {
                case "EMAIL":
                    entry.setEmailStatus(status);
                    entry.setEmailBlobUrl(blobUrl);
                    break;
                case "PRINT":
                    entry.setPrintStatus(status);
                    entry.setPrintFileUrl(blobUrl);
                    break;
                case "MOBSTAT":
                    entry.setMobstatStatus(status);
                    entry.setMobstatBlobUrl(blobUrl);
                    break;
                case "ARCHIVE":
                    entry.setArchiveStatus(status);
                    entry.setArchiveBlobUrl(blobUrl);
                    break;
            }

            grouped.put(key, entry);
        }

        // Now determine overallStatus for each entry
        for (ProcessedFileEntry entry : grouped.values()) {
            String overallStatus = determineOverallStatus(entry, errorMap);
            entry.setOverallStatus(overallStatus);
        }

        return new ArrayList<>(grouped.values());
    }

    private static String determineOverallStatus(ProcessedFileEntry entry, Map<String, Map<String, String>> errorMap) {
        String email = safeStatus(entry.getEmailStatus());
        String print = safeStatus(entry.getPrintStatus());
        String mobstat = safeStatus(entry.getMobstatStatus());
        String archive = safeStatus(entry.getArchiveStatus());

        String customerId = entry.getCustomerId();
        String accountNumber = entry.getAccountNumber();

        boolean hasFailed = false;
        boolean hasSuccess = false;

        List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);
        for (String status : allStatuses) {
            if ("SUCCESS".equals(status)) hasSuccess = true;
            if ("FAILED".equals(status)) hasFailed = true;
        }

        // Generic check: if at least two output methods have SUCCESS, and rest are "", it's SUCCESS
        long nonBlankCount = allStatuses.stream().filter(s -> !s.isEmpty()).count();
        long successCount = allStatuses.stream().filter(s -> "SUCCESS".equals(s)).count();
        if (successCount >= 2 && successCount == nonBlankCount) {
            return "SUCCESS";
        }

        // If any method is FAILED, it's at least PARTIAL
        if (hasFailed) {
            return "PARTIAL";
        }

        // Check errorMap for entries with same customer + account
        Map<String, String> errorEntries = errorMap.getOrDefault(customerId + "-" + accountNumber, Collections.emptyMap());

        for (Map.Entry<String, String> e : errorEntries.entrySet()) {
            String method = e.getKey();
            String error = e.getValue();
            if (("EMAIL".equalsIgnoreCase(method) && email.isEmpty())
                    || ("PRINT".equalsIgnoreCase(method) && print.isEmpty())
                    || ("MOBSTAT".equalsIgnoreCase(method) && mobstat.isEmpty())) {
                return "PARTIAL";
            }
        }

        // Default SUCCESS if archive is SUCCESS and other methods are empty
        if ("SUCCESS".equals(archive) &&
                email.isEmpty() && print.isEmpty() && mobstat.isEmpty()) {
            return "SUCCESS";
        }

        return "PARTIAL";
    }

    private static String safeStatus(String status) {
        return status != null ? status.trim().toUpperCase() : "";
    }
}
