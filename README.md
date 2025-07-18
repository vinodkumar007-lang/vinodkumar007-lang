package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

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

        // METADATA
        Metadata metadata = new Metadata();
        metadata.setTotalFilesProcessed(processedList.size());
        metadata.setProcessingStatus("Completed");
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        payload.setMetadata(metadata);

        // PAYLOAD BLOCK
        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(null);
        payloadInfo.setRunPriority(null);
        payloadInfo.setEventID(null);
        payloadInfo.setEventType(null);
        payloadInfo.setRestartKey(null);
        payloadInfo.setFileCount(processedList.size());
        payload.setPayload(payloadInfo);

        // ✅ Final Processed Entries
        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
        payload.setProcessedFileList(processedFileEntries);

        // ✅ Trigger
        payload.setMobstatTriggerFile(buildMobstatTrigger(processedList));

        return payload;
    }

    private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
        Map<String, ProcessedFileEntry> entryMap = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedList) {
            if (file.getCustomerId() == null || file.getAccountNumber() == null) continue;

            String key = file.getCustomerId() + "::" + file.getAccountNumber();
            ProcessedFileEntry entry = entryMap.computeIfAbsent(key, k -> {
                ProcessedFileEntry e = new ProcessedFileEntry();
                e.setCustomerId(file.getCustomerId());
                e.setAccountNumber(file.getAccountNumber());
                return e;
            });

            String url = file.getBlobURL();
            if (url == null) continue;

            String decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8);

            if (decodedUrl.contains("/email/")) {
                entry.setPdfEmailFileUrl(decodedUrl);
                entry.setPdfEmailFileUrlStatus(file.getStatus());
            } else if (decodedUrl.contains("/archive/")) {
                entry.setPdfArchiveFileUrl(decodedUrl);
                entry.setPdfArchiveFileUrlStatus(file.getStatus());
            } else if (decodedUrl.contains("/mobstat/")) {
                entry.setPdfMobstatFileUrl(decodedUrl);
                entry.setPdfMobstatFileUrlStatus(file.getStatus());
            } else if (decodedUrl.contains("/print/")) {
                entry.setPrintFileUrl(decodedUrl);
                entry.setPrintFileUrlStatus(file.getStatus());
            }

            // Set overallStatus only if not already set
            if (entry.getOverallStatus() == null && file.getOverallStatus() != null) {
                entry.setOverallStatus(file.getOverallStatus());
            }
        }

        return new ArrayList<>(entryMap.values());
    }

    private static String buildMobstatTrigger(List<SummaryProcessedFile> list) {
        return list.stream()
                .map(SummaryProcessedFile::getBlobURL)
                .filter(url -> url != null && url.contains("/DropData.trigger"))
                .findFirst()
                .orElse(null);
    }
}
