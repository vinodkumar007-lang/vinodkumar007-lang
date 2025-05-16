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

        Map<String, Object> summaryResponse = null;

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing record with key: {}, value: {}", record.key(), record.value());
                summaryResponse = handleMessage(record.value());
                break; // Only one message per call
            }
        } catch (Exception e) {
            logger.error("Error polling or processing Kafka record: {}", e.getMessage(), e);
        } finally {
            consumer.close();
        }

        return summaryResponse;
    }

    private Map<String, Object> handleMessage(String message) throws Exception {
        JsonNode root;

        System.out.println("Message check   " + message);
        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            logger.warn("Failed to parse JSON, attempting to convert POJO-like message: {}", message);
            message = convertPojoToJson(message);
            try {
                root = objectMapper.readTree(message);
            } catch (Exception retryEx) {
                logger.error("Failed to parse corrected JSON: {}", retryEx.getMessage(), retryEx);
                return null;
            }
        }

        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            logger.warn("Failed to parse JSON, attempting POJO conversion");

            PublishEvent event = parseToPublishEvent(message);
            // Manually map `event` into a JsonNode if needed:
            root = objectMapper.valueToTree(event);
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
        String summaryMessage = objectMapper.writeValueAsString(summaryResponse);
        assert batchId != null;
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

        return objectMapper.convertValue(summary, Map.class);
    }

    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        return lastDotIndex > 0 ? fileLocation.substring(lastDotIndex) : "";
    }

    /**
     * Attempts to convert a Java-style toString-like object into a JSON string.
     */
    private String convertPojoToJson(String raw) {
        // Remove the class wrapper
        raw = raw.trim();
        if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
            raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
        }

        // Replace '=' with ':' and quote keys and string values
        raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
        raw = raw.replaceAll(":([a-zA-Z0-9_]+)", ":\"$1\"");

        // Quote string values
        raw = raw.replaceAll("([a-zA-Z0-9_]+)", "\"$1\"");

        // Fix nested objectId and similar internal braces
        raw = raw.replaceAll("objectId=\\{([^}]+)}", "\"objectId\": \"$1\"");

        // Fix lists
        raw = raw.replace("],", "], ");

        return "{" + raw + "}";
    }
    public PublishEvent parseToPublishEvent(String raw) {
        try {
            // Step 1: Strip the outer PublishEvent(...)
            if (raw.startsWith("PublishEvent(")) {
                raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
            }

            // Step 2: Replace BatchFile(...) with proper JSON object
            raw = raw.replaceAll("BatchFile\\(", "\\{").replaceAll("\\)", "}");

            // Step 3: Quote objectId content inside {}
            raw = raw.replaceAll("objectId=\\{([^}]+)}", "objectId:\"$1\"");

            // Step 4: Quote keys and values properly (use non-greedy match for values)
            raw = raw.replaceAll("(\\w+)=([^,\\[{]+)", "\"$1\":\"$2\"");

            // Step 5: Fix batchFiles array brackets
            raw = raw.replace("\"batchFiles\":\"[", "\"batchFiles\":[");
            raw = raw.replace("]\"", "]");

            // Step 6: Add JSON braces
            String json = "{" + raw + "}";

            // Step 7: Use Jackson ObjectMapper to parse
            ObjectMapper mapper = new ObjectMapper();
            mapper.findAndRegisterModules(); // for ZonedDateTime, etc.
            return mapper.readValue(json, PublishEvent.class);

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse to PublishEvent", e);
        }
    }
}
