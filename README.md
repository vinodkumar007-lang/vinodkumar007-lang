@PostMapping("/process")
public Map<String, Object> triggerFileProcessing() {
    logger.info("POST /process called to trigger Kafka message processing.");
    return kafkaListenerService.processAllMessages();
}

package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
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

    // ✅ New Method: Process all messages grouped by batchId and create final summary file
    public Map<String, Object> processAllMessages() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(inputTopic);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }

        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);

        Map<String, List<JsonNode>> groupedMessages = new HashMap<>();
        List<String> failedMessages = new ArrayList<>();
        List<Map<String, Object>> summaries = new ArrayList<>();

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            logger.info("Polled {} messages from topic: {}", records.count(), inputTopic);

            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode root;
                    try {
                        root = objectMapper.readTree(record.value());
                    } catch (Exception e) {
                        PublishEvent event = parseToPublishEvent(record.value());
                        root = objectMapper.valueToTree(event);
                    }

                    String batchId = extractField(root, "consumerReference");
                    if (batchId == null) {
                        logger.warn("Missing consumerReference in message, skipping.");
                        failedMessages.add(record.value());
                        continue;
                    }

                    groupedMessages.computeIfAbsent(batchId, k -> new ArrayList<>()).add(root);

                } catch (Exception e) {
                    logger.error("Error parsing/processing message: {}", record.value(), e);
                    failedMessages.add(record.value());
                }
            }

            for (Map.Entry<String, List<JsonNode>> entry : groupedMessages.entrySet()) {
                String batchId = entry.getKey();
                List<JsonNode> messages = entry.getValue();

                try {
                    Map<String, Object> summary = processGroupedBatch(batchId, messages);
                    summaries.add(summary);
                } catch (Exception e) {
                    logger.error("Failed to process grouped batch for batchId {}: {}", batchId, e.getMessage(), e);
                    failedMessages.add("BatchId: " + batchId);
                }
            }

        } finally {
            consumer.close();
        }

        // ✅ Final summary file upload
        String finalSummaryUrl = uploadFinalSummaryFile(summaries);

        Map<String, Object> response = new HashMap<>();
        response.put("processedBatchCount", summaries.size());
        response.put("failedCount", failedMessages.size());
        response.put("summaryByBatchId", summaries);
        response.put("failedMessages", failedMessages);
        response.put("finalSummaryFileUrl", finalSummaryUrl);

        return response;
    }

    // ✅ Group-based batch processing
    private Map<String, Object> processGroupedBatch(String batchId, List<JsonNode> messages) throws Exception {
        List<JsonNode> allBatchFiles = new ArrayList<>();

        for (JsonNode message : messages) {
            JsonNode batchFilesNode = message.get("batchFiles");
            if (batchFilesNode != null && batchFilesNode.isArray()) {
                for (JsonNode file : batchFilesNode) {
                    allBatchFiles.add(file);
                }
            }
        }

        if (allBatchFiles.isEmpty()) {
            logger.warn("No batch files found for batchId: {}", batchId);
            return null;
        }

        JsonNode firstFile = allBatchFiles.get(0);
        String filePath = firstFile.get("fileLocation").asText();
        String objectId = firstFile.get("ObjectId").asText();

        String blobUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
        logger.info("Batch {} uploaded to blob storage at URL: {}", batchId, blobUrl);

        JsonNode batchFilesNode = objectMapper.valueToTree(allBatchFiles);

        Map<String, Object> summary = buildSummaryPayload(batchId, blobUrl, batchFilesNode);
        String summaryMessage = objectMapper.writeValueAsString(summary);
        kafkaTemplate.send(outputTopic, batchId, summaryMessage);
        logger.info("Summary for batchId {} published to topic {}: {}", batchId, outputTopic, summaryMessage);

        return summary;
    }

    // ✅ Upload final summary file
    private String uploadFinalSummaryFile(List<Map<String, Object>> batchSummaries) {
        try {
            String timestamp = new Date().toInstant().toString().replace(":", "_");
            String fileName = "final_summary_" + timestamp + ".json";
            String fileContent = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(batchSummaries);

            return blobStorageService.uploadSummaryFileAndGenerateSasUrl(fileName, fileContent);
        } catch (Exception e) {
            logger.error("Failed to upload final summary file: {}", e.getMessage(), e);
            return "ERROR: Could not upload summary file.";
        }
    }

    // ✅ Your existing single-message trigger (unchanged)
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
                break;
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

        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            logger.warn("Failed to parse JSON, attempting POJO conversion: {}", message);
            PublishEvent event = parseToPublishEvent(message);
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

    private String convertPojoToJson(String raw) {
        raw = raw.trim();
        if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
            raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
        }

        raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
        raw = raw.replaceAll(":([a-zA-Z0-9_]+)", ":\"$1\"");
        raw = raw.replaceAll("([a-zA-Z0-9_]+)", "\"$1\"");
        raw = raw.replaceAll("objectId=\\{([^}]+)}", "\"objectId\": \"$1\"");
        raw = raw.replace("],", "], ");
        return "{" + raw + "}";
    }

    public PublishEvent parseToPublishEvent(String raw) {
        try {
            if (raw.startsWith("PublishEvent(")) {
                raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
            }

            raw = raw.replaceAll("BatchFile\\(", "\\{").replaceAll("\\)", "}");
            raw = raw.replaceAll("objectId=\\{([^}]+)}", "objectId:\"$1\"");
            raw = raw.replaceAll("(\\w+)=([^,\\[{]+)", "\"$1\":\"$2\"");
            raw = raw.replace("\"batchFiles\":\"[", "\"batchFiles\":[");
            raw = raw.replace("]\"", "]");
            String json = "{" + raw + "}";

            ObjectMapper mapper = new ObjectMapper();
            mapper.findAndRegisterModules();
            return mapper.readValue(json, PublishEvent.class);

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse to PublishEvent", e);
        }
    }
}
