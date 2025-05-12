package com.nedbank.kafka.filemanage.service;

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
            // Extract necessary fields from the incoming Kafka message
            String batchId = extractField(message, "ecpBatchGuid");  // Extracting ecpBatchGuid as batchId
            String filePath = extractField(message, "blobInputId");

            logger.info("Parsed batchId: {}, filePath: {}", batchId, filePath);

            // Upload the file and generate the SAS URL
            String blobUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId);
            logger.info("File uploaded to blob storage at URL: {}", blobUrl);

            // Build and send the summary payload to the output Kafka topic
            Map<String, Object> summaryPayload = buildSummaryPayload(batchId, blobUrl);
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
    private String extractField(String json, String fieldName) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readTree(json).get(fieldName).asText();
        } catch (Exception e) {
            // Detailed logging for missing or incorrect fields
            logger.error("Failed to extract field '{}'. Error: {}", fieldName, e.getMessage(), e);
            throw new RuntimeException("Failed to extract " + fieldName + " from message: " + json, e);
        }
    }

    /**
     * Builds the summary payload to send to the output Kafka topic
     * @param batchId the batch ID
     * @param blobUrl the URL of the uploaded file in blob storage
     * @return a map containing the summary payload
     */
    private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl) {
        // Creating the processed files list
        List<ProcessedFileInfo> processedFiles = List.of(
                new ProcessedFileInfo("C001", blobUrl + "/pdfs/C001_" + batchId + ".pdf"),
                new ProcessedFileInfo("C002", blobUrl + "/pdfs/C002_" + batchId + ".pdf")
        );

        // Constructing the summary payload
        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(batchId);
        summary.setHeader(new HeaderInfo()); // Populate header if required
        summary.setMetadata(new MetadataInfo()); // Populate metadata if required
        summary.setPayload(new PayloadInfo()); // Populate payload if required
        summary.setProcessedFiles(processedFiles);
        summary.setSummaryFileURL(blobUrl + "/summary/" + batchId + "_summary.json");

        // Convert the summary to a Map for easy sending to Kafka
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(summary, Map.class);
    }
}
Received Kafka message: {
  "sourceSystem" : "DEBTMAN",
  "timestamp" : 1747049498.879427900,
  "batchFiles" : [ {
    "fileLocation" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH"
  } ],
  "consumerReference" : "12345",
  "processReference" : "Test12345",
  "batchControlFileData" : null
}
2025-05-12T13:34:51.564+02:00 ERROR 17416 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Failed to extract field 'ecpBatchGuid'. Error: Cannot invoke "com.fasterxml.jackson.databind.JsonNode.asText()" because the return value of "com.fasterxml.jackson.databind.JsonNode.get(String)" is null

java.lang.NullPointerException: Cannot invoke "com.fasterxml.jackson.databind.JsonNode.asText()" because the return value of "com.fasterxml.jackson.databind.JsonNode.get(String)" is null
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.extractField(KafkaListenerService.java:77) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:46) ~[classes/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:169)
