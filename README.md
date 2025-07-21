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

        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap);
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

    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedList,
            Map<String, Map<String, String>> errorMap) {

        List<ProcessedFileEntry> finalList = new ArrayList<>();

        // Define mapping of grouped output types to primary label
        Map<Set<String>, String> groupTypeToLabel = new HashMap<>();
        groupTypeToLabel.put(new HashSet<>(Arrays.asList("EMAIL", "ARCHIVE")), "EMAIL");
        groupTypeToLabel.put(new HashSet<>(Arrays.asList("PRINT", "ARCHIVE")), "PRINT");
        groupTypeToLabel.put(new HashSet<>(Arrays.asList("MOBSTAT", "ARCHIVE")), "MOBSTAT");
        groupTypeToLabel.put(new HashSet<>(Arrays.asList("HTML", "ARCHIVE")), "HTML"); // Add more if needed

        // Group by customerId + accountNumber
        Map<String, List<SummaryProcessedFile>> grouped =
                processedList.stream()
                        .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
                        .collect(Collectors.groupingBy(f ->
                                f.getCustomerId() + "::" + f.getAccountNumber()));

        for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
            String[] parts = entry.getKey().split("::");
            String customerId = parts[0];
            String accountNumber = parts[1];

            List<SummaryProcessedFile> files = entry.getValue();

            // Build map of outputType -> blobUrl
            Map<String, String> typeToUrl = new HashMap<>();
            for (SummaryProcessedFile file : files) {
                typeToUrl.put(file.getOutputType(), file.getBlobURL());
            }

            // For each defined group combination (EMAIL+ARCHIVE, PRINT+ARCHIVE, etc.)
            for (Map.Entry<Set<String>, String> groupEntry : groupTypeToLabel.entrySet()) {
                Set<String> combo = groupEntry.getKey(); // e.g. [EMAIL, ARCHIVE]
                String outputTypeLabel = groupEntry.getValue(); // e.g. EMAIL

                if (!typeToUrl.keySet().containsAll(combo)) {
                    // If not present in actual uploaded outputTypes, skip
                    continue;
                }

                // Check if all URLs are present
                boolean allSuccess = combo.stream().allMatch(t -> typeToUrl.get(t) != null);

                String status;
                if (allSuccess) {
                    status = "SUCCESS";
                } else {
                    boolean anyFailed = combo.stream().anyMatch(t -> {
                        String errKey = customerId + "::" + accountNumber;
                        return errorMap.containsKey(errKey) &&
                                errorMap.get(errKey).getOrDefault(t, "").equalsIgnoreCase("Failed");
                    });

                    if (anyFailed) {
                        status = "FAILED";
                    } else {
                        status = "PARTIAL";
                    }
                }

                // Pick primary URL (like EMAIL or PRINT), fallback to ARCHIVE if primary null
                String mainBlobUrl = typeToUrl.get(outputTypeLabel);
                if (mainBlobUrl == null && combo.contains("ARCHIVE")) {
                    mainBlobUrl = typeToUrl.get("ARCHIVE");
                }

                ProcessedFileEntry processedFile = new ProcessedFileEntry();
                processedFile.setCustomerId(customerId);
                processedFile.setAccountNumber(accountNumber);
                processedFile.setOutputType(outputTypeLabel);
                processedFile.setBlobUrl(mainBlobUrl);
                processedFile.setStatus(status);

                finalList.add(processedFile);
            }
        }

        return finalList;
    }
}
