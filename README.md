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

        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
        payload.setProcessedFileList(processedFileEntries);

        int totalFileUrls = processedFileEntries.size();

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

    private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles) {
        Map<String, List<SummaryProcessedFile>> grouped = processedFiles.stream()
                .collect(Collectors.groupingBy(p -> p.getCustomerId() + "|" + p.getAccountNumber()));

        List<ProcessedFileEntry> result = new ArrayList<>();

        for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
            List<SummaryProcessedFile> groupList = entry.getValue();

            if (groupList.isEmpty()) continue;

            SummaryProcessedFile first = groupList.get(0);

            ProcessedFileEntry processedEntry = new ProcessedFileEntry();
            processedEntry.setCustomerId(first.getCustomerId());
            processedEntry.setAccountNumber(first.getAccountNumber());

            String overallStatus = "SUCCESS";

            for (SummaryProcessedFile file : groupList) {
                switch (file.getOutputType()) {
                    case "EMAIL":
                        processedEntry.setPdfEmailFileUrl(file.getBlobURL());
                        processedEntry.setPdfEmailFileUrl(file.getStatus());
                        break;
                    case "MOBSTAT":
                        processedEntry.setPdfMobstatFileUrl(file.getBlobURL());
                        processedEntry.setPdfMobstatFileUrlStatus(file.getStatus());
                        break;
                    case "PRINT":
                        processedEntry.setPrintFileUrl(file.getBlobURL());
                        processedEntry.setPrintFileUrlStatus(file.getStatus());
                        break;
                }

                // Always one archive per group
                processedEntry.setArchiveBlobUrl(file.getArchiveBlobUrl());
                processedEntry.setArchiveStatus(file.getArchiveStatus());

                // Compute overall status (any FAILED → FAILED, any PARTIAL → PARTIAL)
                if ("FAILED".equalsIgnoreCase(file.getStatus())) {
                    overallStatus = "FAILED";
                } else if ("PARTIAL".equalsIgnoreCase(file.getStatus()) && !"FAILED".equalsIgnoreCase(overallStatus)) {
                    overallStatus = "PARTIAL";
                }
            }

            processedEntry.setOverallStatus(overallStatus);
            result.add(processedEntry);
        }

        return result;
    }
}
