package com.example.demo.listener;

import com.example.demo.service.BlobStorageService;
import com.example.demo.service.SummaryJsonWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;
    private final BlobStorageService blobStorageService;
    private final SummaryJsonWriter summaryJsonWriter;

    private final Set<String> processedMessageIds = new HashSet<>();

    public KafkaListenerService(KafkaConsumer<String, String> consumer,
                                ObjectMapper objectMapper,
                                BlobStorageService blobStorageService,
                                SummaryJsonWriter summaryJsonWriter) {
        this.consumer = consumer;
        this.objectMapper = objectMapper;
        this.blobStorageService = blobStorageService;
        this.summaryJsonWriter = summaryJsonWriter;
    }

    public Map<String, Object> listen() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        List<Map<String, Object>> processedFiles = new ArrayList<>();
        List<Map<String, Object>> printFiles = new ArrayList<>();
        Map<String, Object> header = new HashMap<>();
        String batchId = UUID.randomUUID().toString();
        String fileName = "summary.json";

        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();

            try {
                JsonNode rootNode = objectMapper.readTree(message);
                String messageId = rootNode.path("id").asText();

                if (processedMessageIds.contains(messageId)) {
                    continue;
                }

                processedMessageIds.add(messageId);

                JsonNode dataNode = rootNode.path("data");

                String blobUrl = dataNode.path("blobUrl").asText();
                String sourceSystem = dataNode.path("sourceSystem").asText();
                String objectId = dataNode.path("objectId").asText();
                String consumerReference = dataNode.path("consumerReference").asText();
                String processReference = dataNode.path("processReference").asText();
                String timestamp = dataNode.path("timestamp").asText();
                String eventOutcomeCode = dataNode.path("eventOutcomeCode").asText();
                String eventOutcomeDescription = dataNode.path("eventOutcomeDescription").asText();

                String destPath = batchId + "/" + objectId;

                // Correct call to copy file to Azure Blob
                Map<String, String> metadata = new HashMap<>();
                metadata.put("sourceSystem", sourceSystem);
                metadata.put("consumerReference", consumerReference);
                metadata.put("processReference", processReference);
                metadata.put("timestamp", timestamp);

                try {
                    blobStorageService.copyFileFromUrlToBlob(blobUrl, destPath, metadata);
                } catch (Exception e) {
                    logger.warn("Failed to copy blob from URL {}: {}", blobUrl, e.getMessage());
                }

                Map<String, Object> processedFileEntry = new HashMap<>();
                processedFileEntry.put("blobUrl", blobUrl);
                processedFileEntry.put("sourceSystem", sourceSystem);
                processedFileEntry.put("objectId", objectId);
                processedFileEntry.put("consumerReference", consumerReference);
                processedFileEntry.put("processReference", processReference);
                processedFileEntry.put("timestamp", timestamp);
                processedFileEntry.put("eventOutcomeCode", eventOutcomeCode);
                processedFileEntry.put("eventOutcomeDescription", eventOutcomeDescription);

                processedFiles.add(processedFileEntry);

                // Optional: Add print file if present
                if (dataNode.has("printFileURL")) {
                    Map<String, Object> printEntry = new HashMap<>();
                    printEntry.put("printFileURL", dataNode.path("printFileURL").asText());
                    printFiles.add(printEntry);
                }

                // Populate header fields only once
                if (header.isEmpty()) {
                    header.put("batchID", batchId);
                    header.put("fileName", fileName);
                    header.put("timestamp", Instant.now().toString());
                }

            } catch (Exception e) {
                logger.error("Failed to process Kafka message: {}", e.getMessage(), e);
            }
        }

        File summaryFile = null;
        String summaryBlobUrl = null;
        Map<String, Object> response = new HashMap<>();

        if (!processedFiles.isEmpty()) {
            summaryFile = summaryJsonWriter.appendToSummaryJson(batchId, header, processedFiles, printFiles);

            // Correct call to upload summary.json to Azure
            try {
                String summaryDestPath = "summaries/" + UUID.randomUUID() + "-summary.json";
                summaryBlobUrl = blobStorageService.uploadSummaryJson(summaryFile, summaryDestPath);
            } catch (Exception e) {
                logger.error("Failed to upload summary.json: {}", e.getMessage(), e);
            }

            Map<String, Object> payload = new HashMap<>();
            payload.put("header", header);
            payload.put("processedFiles", processedFiles);
            payload.put("printFiles", printFiles);
            payload.put("summaryFileURL", summaryBlobUrl);

            response.put("message", "Kafka messages processed successfully");
            response.put("status", "success");
            response.put("summaryPayload", payload);
        } else {
            response.put("message", "No new Kafka messages to process");
            response.put("status", "success");
        }

        return response;
    }
}
