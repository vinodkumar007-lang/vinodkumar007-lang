package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ConsumerFactory<String, String> consumerFactory;

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    // Manual or scheduled processing
    public Map<String, Object> processSingleMessage() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        consumer.assign(Collections.singletonList(new TopicPartition(inputTopic, 0)));
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition(inputTopic, 0)));

        Map<String, Object> summaryResponse = null;

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing record with key: {}, value: {}", record.key(), record.value());
                summaryResponse = handleMessage(record.value());
                break; // Process only one message
            }
        } catch (Exception e) {
            logger.error("Error polling or processing Kafka record: {}", e.getMessage(), e);
        } finally {
            consumer.close();
        }

        return summaryResponse;
    }

    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void scheduledFileProcessing() {
        logger.info("Scheduled Kafka message processing triggered.");
        processSingleMessage();
    }

    private Map<String, Object> handleMessage(String message) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = tryParseJson(message); // Attempt to parse the message

        if (root == null) {
            // If parsing failed, try to fix and re-parse
            logger.warn("Invalid JSON detected. Attempting to fix the JSON.");
            String fixedMessage = fixInvalidJson(message);
            root = tryParseJson(fixedMessage); // Attempt to parse the fixed message
            if (root == null) {
                logger.error("Failed to fix the invalid JSON message: {}", message);
                return null;
            }
        }

        String batchId = extractField(root, "consumerReference");
        JsonNode batchFilesNode = root.get("batchFiles");

        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            logger.warn("No batch files found in the message.");
            return null;
        }

        JsonNode firstFile = batchFilesNode.get(0);
        String filePath = firstFile.get("fileLocation").asText();
        String objectId = firstFile.get("ObjectId").asText();

        logger.info("Parsed batchId: {}, filePath: {}, objectId: {}", batchId, filePath, objectId);

        String blobUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
        logger.info("File uploaded to blob storage at URL: {}", blobUrl);

        Map<String, Object> summaryResponse = buildSummaryPayload(batchId, blobUrl, batchFilesNode);
        String summaryMessage = mapper.writeValueAsString(summaryResponse);
        kafkaTemplate.send(outputTopic, batchId, summaryMessage);
        logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

        return summaryResponse;
    }

    private String extractField(JsonNode json, String fieldName) {
        try {
            JsonNode fieldNode = json.get(fieldName);
            return fieldNode != null ? fieldNode.asText() : null;
        } catch (Exception e) {
            logger.error("Failed to extract field '{}': {}", fieldName, e.getMessage(), e);
            throw new RuntimeException("Failed to extract " + fieldName, e);
        }
    }

    private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl, JsonNode batchFilesNode) {
        List<ProcessedFileInfo> processedFiles = new ArrayList<>();

        for (JsonNode fileNode : batchFilesNode) {
            String objectId = fileNode.get("ObjectId").asText();
            String fileLocation = fileNode.get("fileLocation").asText();
            String extension = getFileExtension(fileLocation);

            String dynamicFileUrl = blobUrl + "/" + objectId.replaceAll("[{}]", "") + "_" + batchId + "_" + objectId + extension;
            processedFiles.add(new ProcessedFileInfo(objectId, dynamicFileUrl));
        }

        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(batchId);
        summary.setHeader(new HeaderInfo());
        summary.setMetadata(new MetadataInfo());
        summary.setPayload(new PayloadInfo());
        summary.setProcessedFiles(processedFiles);
        summary.setSummaryFileURL(blobUrl + "/summary/" + batchId + "_summary.json");

        return new ObjectMapper().convertValue(summary, Map.class);
    }

    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        return lastDotIndex > 0 ? fileLocation.substring(lastDotIndex) : "";
    }

    // Helper methods to handle JSON parsing and fixing

    /**
     * Attempt to parse a JSON string and return JsonNode
     * @param message the JSON message string
     * @return JsonNode if valid JSON, null if invalid
     */
    private JsonNode tryParseJson(String message) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(message);
        } catch (Exception e) {
            return null; // Return null if JSON parsing fails
        }
    }

    /**
     * Fix invalid JSON by adding quotes around keys and values.
     * @param message the invalid JSON message
     * @return the fixed JSON message
     */
    private String fixInvalidJson(String message) {
        // Example fixes for common issues:
        // - Add quotes around unquoted keys and values
        message = message.replaceAll("([a-zA-Z0-9]+):", "\"$1\":");
        message = message.replaceAll(":(\\w+)", ":\"$1\"");

        return message;
    }
}
