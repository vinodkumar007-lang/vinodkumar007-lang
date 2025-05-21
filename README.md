package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ConsumerFactory<String, String> consumerFactory;
    private final ObjectMapper objectMapper = new ObjectMapper();

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

    public Map<String, Object> processAllMessages() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        consumer.assign(Collections.singletonList(new TopicPartition(inputTopic, 0)));
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition(inputTopic, 0)));

        Map<String, Object> summaryResponse = new HashMap<>();
        List<Map<String, Object>> processedBatchSummaries = new ArrayList<>();

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing record with key: {}, value: {}", record.key(), record.value());
                Map<String, Object> recordSummary = handleMessage(record.value());
                if (recordSummary != null) {
                    processedBatchSummaries.add(recordSummary);
                }
            }

            if (!processedBatchSummaries.isEmpty()) {
                summaryResponse.put("processedBatchSummaries", processedBatchSummaries);
                logger.info("Processed {} messages from Kafka topic: {}", processedBatchSummaries.size(), inputTopic);
            }
        } catch (Exception e) {
            logger.error("Error polling or processing Kafka record: {}", e.getMessage(), e);
            summaryResponse.put("status", "500");
            summaryResponse.put("message", "Internal Server Error while processing messages");
        } finally {
            consumer.close();
        }

        return summaryResponse;
    }

    private Map<String, Object> handleMessage(String message) throws Exception {
        JsonNode root;

        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            logger.warn("Failed to parse JSON, attempting fallback conversion.");
            message = convertPojoToJson(message);
            try {
                root = objectMapper.readTree(message);
            } catch (Exception retryEx) {
                logger.error("Fallback JSON parsing failed: {}", retryEx.getMessage(), retryEx);
                return generateErrorResponse("400", "Invalid message format");
            }
        }

        String batchId = extractField(root, "consumerReference");
        JsonNode batchFilesNode = root.get("batchFiles");

        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            logger.warn("No batch files found.");
            return generateErrorResponse("400", "No batch files in message");
        }

        JsonNode firstFile = batchFilesNode.get(0);
        String filePath = firstFile.get("fileLocation").asText();
        String objectId = firstFile.get("ObjectId").asText();

        logger.info("Parsed batchId: {}, filePath: {}, objectId: {}", batchId, filePath, objectId);

        String sasUrl;
        try {
            sasUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
        } catch (Exception e) {
            logger.error("File upload failed: {}", e.getMessage());
            sasUrl = null; // fallback to summary file in C drive
        }

        // Build summary JSON and save it
        String summaryFileUrl = writeSummaryToLocalDrive(batchId, batchFilesNode);

        SummaryPayload summaryPayload = buildSummaryPayload(batchId, summaryFileUrl, batchFilesNode);

        // Send summary message to Kafka
        String summaryMessage = objectMapper.writeValueAsString(summaryPayload);
        kafkaTemplate.send(outputTopic, batchId, summaryMessage);
        logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

        Map<String, Object> response = new HashMap<>();
        response.put("batchId", batchId);
        response.put("summaryFileURL", summaryFileUrl);
        response.put("processedFiles", summaryPayload.getProcessedFiles());
        response.put("header", summaryPayload.getHeader());
        response.put("metadata", summaryPayload.getMetadata());
        response.put("payload", summaryPayload.getPayload());

        return response;
    }

    private SummaryPayload buildSummaryPayload(String batchId, String summaryFileUrl, JsonNode batchFilesNode) {
        List<ProcessedFileInfo> processedFiles = new ArrayList<>();

        for (JsonNode fileNode : batchFilesNode) {
            String objectId = fileNode.get("ObjectId").asText();
            String fileLocation = fileNode.get("fileLocation").asText();
            String fileUrl = "file://" + fileLocation;
            processedFiles.add(new ProcessedFileInfo(objectId, fileUrl));
        }

        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(batchId);
        summary.setHeader(new HeaderInfo());
        summary.setMetadata(new MetadataInfo());
        summary.setPayload(new PayloadInfo());
        summary.setProcessedFiles(processedFiles);
        summary.setSummaryFileURL(summaryFileUrl);
        return summary;
    }

    private String writeSummaryToLocalDrive(String batchId, JsonNode batchFilesNode) {
        try {
            SummaryFileInfo summaryFileInfo = new SummaryFileInfo();
            summaryFileInfo.setBatchId(batchId);
            summaryFileInfo.setTimestamp(new Date().toString());
            summaryFileInfo.setFiles(objectMapper.convertValue(batchFilesNode, List.class));

            String filePath = "C:\\summary_outputs\\summary_" + batchId + ".json";
            File outputDir = new File("C:\\summary_outputs");
            if (!outputDir.exists()) outputDir.mkdirs();

            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(filePath), summaryFileInfo);
            return "file:///" + filePath.replace("\\", "/");
        } catch (IOException e) {
            logger.error("Error writing summary.json: {}", e.getMessage(), e);
            return null;
        }
    }

    private String extractField(JsonNode json, String fieldName) {
        try {
            JsonNode fieldNode = json.get(fieldName);
            return fieldNode != null ? fieldNode.asText() : null;
        } catch (Exception e) {
            logger.error("Failed to extract field '{}': {}", fieldName, e.getMessage(), e);
            return null;
        }
    }

    private String convertPojoToJson(String raw) {
        raw = raw.trim();
        if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
            raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
        }
        raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
        raw = raw.replaceAll(":([a-zA-Z0-9_]+)", ":\"$1\"");
        return "{" + raw + "}";
    }

    private Map<String, Object> generateErrorResponse(String errorCode, String errorMessage) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("status", errorCode);
        errorResponse.put("message", errorMessage);
        return errorResponse;
    }
}
