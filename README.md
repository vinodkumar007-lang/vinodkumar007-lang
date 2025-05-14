package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate, BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

    /**
     * Kafka Listener that consumes messages from the input topic
     * @param record the Kafka message record
     */
    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void consumeKafkaMessage(ConsumerRecord<String, String> record) {
        logger.info("Kafka listener method entered.");
        String message = record.value();
        logger.info("Received Kafka message: {}", message);

        try {
            // Parse the incoming Kafka message
            JsonNode root = new ObjectMapper().readTree(message);

            // Extract necessary fields from the incoming Kafka message
            String batchId = extractField(root, "consumerReference");  // Using consumerReference as batchId
            JsonNode batchFilesNode = root.get("batchFiles");

            if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
                logger.warn("No batch files found in the message.");
                return;
            }

            // For now, take the first file entry for blob upload
            JsonNode firstFile = batchFilesNode.get(0);
            String filePath = firstFile.get("fileLocation").asText();
            String objectId = firstFile.get("ObjectId").asText();

            logger.info("Parsed batchId: {}, filePath: {}, objectId: {}", batchId, filePath, objectId);

            // Upload the file and generate the SAS URL
            String blobUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
            logger.info("File uploaded to blob storage at URL: {}", blobUrl);

            // Build and send the summary payload to the output Kafka topic
            Map<String, Object> summaryPayload = buildSummaryPayload(batchId, blobUrl, batchFilesNode);
            String summaryMessage = new ObjectMapper().writeValueAsString(summaryPayload);

            kafkaTemplate.send(outputTopic, batchId, summaryMessage);
            logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

        } catch (Exception e) {
            // Improved error handling with detailed logging
            logger.error("Error processing Kafka message: {}. Error: {}", message, e.getMessage(), e);
        }
    }

    /**
     * Extracts a field from the Kafka message
     * @param json the raw Kafka message in JSON format
     * @param fieldName the field to extract from the JSON
     * @return the value of the field
     */
    private String extractField(JsonNode json, String fieldName) {
        try {
            JsonNode fieldNode = json.get(fieldName);
            if (fieldNode != null) {
                return fieldNode.asText();
            } else {
                logger.warn("Field '{}' not found in the message", fieldName);
                return null;
            }
        } catch (Exception e) {
            logger.error("Failed to extract field '{}'. Error: {}", fieldName, e.getMessage(), e);
            throw new RuntimeException("Failed to extract " + fieldName + " from message", e);
        }
    }

    /**
     * Builds the summary payload to send to the output Kafka topic
     * @param batchId the batch ID
     * @param blobUrl the URL of the uploaded file in blob storage
     * @param batchFilesNode the batchFiles node from the Kafka message
     * @return a map containing the summary payload
     */
    private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl, JsonNode batchFilesNode) {
        List<ProcessedFileInfo> processedFiles = new ArrayList<>();

        for (JsonNode fileNode : batchFilesNode) {
            String objectId = fileNode.get("ObjectId").asText();
            String fileLocation = fileNode.get("fileLocation").asText();
            String extension = getFileExtension(fileLocation);

            String dynamicFileUrl = blobUrl + "/" + objectId.replaceAll("[{}]", "") + "_" + batchId + extension;
            processedFiles.add(new ProcessedFileInfo(objectId, dynamicFileUrl));
        }

        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(batchId);
        summary.setHeader(new HeaderInfo()); // Populate header if required
        summary.setMetadata(new MetadataInfo()); // Populate metadata if required
        summary.setPayload(new PayloadInfo()); // Populate payload if required
        summary.setProcessedFiles(processedFiles);
        summary.setSummaryFileURL(blobUrl + "/summary/" + batchId + "_summary.json");

        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(summary, Map.class);
    }

    /**
     * Extracts the file extension from a file location URL
     * @param fileLocation the file location URL
     * @return the file extension
     */
    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        if (lastDotIndex > 0) {
            return fileLocation.substring(lastDotIndex);
        } else {
            return ""; // Default to empty string if no extension is found
        }
    }
}

org.springframework.kafka.KafkaException: Send failed
	at org.springframework.kafka.core.KafkaTemplate.doSend(KafkaTemplate.java:794) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.observeSend(KafkaTemplate.java:754) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.send(KafkaTemplate.java:544) ~[spring-kafka-3.0.11.jar:3.0.11]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:73) ~[classes/:na]
	at jdk.internal.reflect.GeneratedMethodAccessor10.invoke(Unknown Source) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:169) ~[spring-messaging-6.0.2.jar:6.0.2]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.invoke(InvocableHandlerMethod.java:119) ~[spring-messaging-6.0.2.jar:6.0.2]
	at org.springframework.kafka.listener.adapter.HandlerAdapter.invoke(HandlerAdapter.java:56) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter.invokeHandler(MessagingMessageListenerAdapter.java:375) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:92) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:53) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeOnMessage(KafkaMessageListenerContainer.java:2873) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeOnMessage(KafkaMessageListenerContainer.java:2854) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.lambda$doInvokeRecordListener$57(KafkaMessageListenerContainer.java:2772) ~[spring-kafka-3.0.11.jar:3.0.11]
	at io.micrometer.observation.Observation.observe(Observation.java:559) ~[micrometer-observation-1.10.2.jar:1.10.2]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeRecordListener(KafkaMessageListenerContainer.java:2770) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeWithRecords(KafkaMessageListenerContainer.java:2622) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeRecordListener(KafkaMessageListenerContainer.java:2508) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeListener(KafkaMessageListenerContainer.java:2150) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeIfHaveRecords(KafkaMessageListenerContainer.java:1505) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.pollAndInvoke(KafkaMessageListenerContainer.java:1469) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.run(KafkaMessageListenerContainer.java:1344) ~[spring-kafka-3.0.11.jar:3.0.11]
	at java.base/java.util.concurrent.CompletableFuture$AsyncRun.run(CompletableFuture.java:1804) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:842) ~[na:na]
Caused by: org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [ecp-batch-composition-complete]
