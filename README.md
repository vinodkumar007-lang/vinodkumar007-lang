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

        Map<String, Object> finalSummaryResponse = new HashMap<>();
        Set<String> batchIds = new HashSet<>();
        List<Map<String, Object>> batchSummaries = new ArrayList<>();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    break; // Exit loop when no more messages are available
                }
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Processing record with key: {}, value: {}", record.key(), record.value());
                    Map<String, Object> batchSummary = handleMessage(record.value());
                    if (batchSummary != null) {
                        batchSummaries.add(batchSummary);
                        batchIds.add((String) batchSummary.get("batchID"));
                    }
                }
            }

            if (!batchSummaries.isEmpty()) {
                String summaryFileUrl = uploadFinalSummaryFile(batchSummaries, batchIds);
                finalSummaryResponse.put("summaryFileUrl", summaryFileUrl);
            }

        } catch (Exception e) {
            logger.error("Error processing Kafka records: {}", e.getMessage(), e);
        } finally {
            consumer.close();
        }

        return finalSummaryResponse;
    }

    private Map<String, Object> handleMessage(String message) throws Exception {
        JsonNode root = objectMapper.readTree(message);
        String batchId = extractField(root, "consumerReference");
        JsonNode batchFilesNode = root.get("batchFiles");

        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            logger.warn("No batch files found in the message.");
            return null;
        }

        String dynamicObjectId = getDynamicObjectId(batchFilesNode);

        if (dynamicObjectId == null) {
            logger.warn("No valid objectId found.");
            return null;
        }

        // Handle file processing logic here (upload, create summary, etc.)
        String blobUrl = blobStorageService.uploadFileAndGenerateSasUrl(batchFilesNode.get(0).get("fileLocation").asText(), batchId, dynamicObjectId);

        Map<String, Object> summaryResponse = buildSummaryPayload(batchId, blobUrl, batchFilesNode);
        kafkaTemplate.send(outputTopic, batchId, objectMapper.writeValueAsString(summaryResponse));
        logger.info("Summary published to Kafka topic: {}", outputTopic);

        return summaryResponse;
    }

    private String getDynamicObjectId(JsonNode batchFilesNode) {
        // Dynamically select objectId from any file in the batchFilesNode
        for (JsonNode fileNode : batchFilesNode) {
            String objectId = fileNode.get("ObjectId").asText();
            if (objectId != null && !objectId.isEmpty()) {
                return objectId; // Return the first valid objectId found
            }
        }
        return null; // Return null if no valid objectId is found
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

    private String uploadFinalSummaryFile(List<Map<String, Object>> batchSummaries, Set<String> batchIds) {
        try {
            if (batchSummaries.isEmpty()) return "No batch summaries to upload.";

            // Collect all unique batch IDs
            List<Map<String, Object>> processedFiles = new ArrayList<>();
            String dynamicObjectId = null;

            // Iterate through all batches to collect batchId and processed files info
            for (Map<String, Object> summary : batchSummaries) {
                List<Map<String, Object>> batchProcessedFiles = (List<Map<String, Object>>) summary.get("processedFiles");
                if (batchProcessedFiles != null && !batchProcessedFiles.isEmpty()) {
                    processedFiles.addAll(batchProcessedFiles);
                }
            }

            // Dynamically get an objectId from the processed files
            dynamicObjectId = getDynamicObjectId(processedFiles);

            // Generate a dynamic file name including all batchIds
            String batchIdsStr = String.join("_", batchIds);
            String fileName = "final_summary_" + batchIdsStr + ".json";

            // Create content with all batch summaries
            String fileContent = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(batchSummaries);

            // If a valid objectId is found, upload the final summary file
            if (dynamicObjectId != null) {
                return blobStorageService.uploadSummaryFileAndGenerateSasUrl(fileName, fileContent, batchIdsStr, dynamicObjectId);
            } else {
                logger.warn("No objectId found for any processed files. Cannot upload final summary.");
                return "ERROR: No objectId found for final summary.";
            }

        } catch (Exception e) {
            logger.error("Failed to upload final summary file: {}", e.getMessage(), e);
            return "ERROR: Could not upload summary file.";
        }
    }
}

package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    @Value("${azure.storage.account-name}")
    private String accountName;

    @Value("${azure.storage.account-key}")
    private String accountKey;

    @Value("${azure.storage.container-name}")
    private String containerName;

    public String uploadFileAndGenerateSasUrl(String fileLocation, String batchId, String objectId) throws IOException {
        try {
            // Set up Azure Blob client
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = containerClient.getBlobClient(objectId + "_" + batchId);

            // Get source blob name from URL
            String sourceBlobName = fileLocation.substring(fileLocation.lastIndexOf("/") + 1);
            BlobClient sourceBlobClient = containerClient.getBlobClient(sourceBlobName);

            // Download and upload to target blob
            try (InputStream inputStream = sourceBlobClient.openQueryStream()) {
                long sourceSize = sourceBlobClient.getProperties().getBlobSize();
                targetBlobClient.upload(inputStream, sourceSize, true); // true = overwrite
                logger.info("Successfully uploaded file '{}' to '{}'", sourceBlobName, targetBlobClient.getBlobUrl());
            }

            // Generate SAS URL
            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                    OffsetDateTime.now().plusHours(24),
                    new BlobSasPermission().setReadPermission(true)
            );

            String sasToken = targetBlobClient.generateSas(sasValues);
            return targetBlobClient.getBlobUrl() + "?" + sasToken;

        } catch (IOException e) {
            logger.error("Error uploading file to blob storage: {}", e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error during blob upload: {}", e.getMessage(), e);
            throw new IOException("Unexpected error during blob upload", e);
        }
    }

    public String uploadSummaryFileAndGenerateSasUrl(String fileName, String fileContent, String batchIds, String objectId) throws IOException {
        // Logic for uploading summary file with batch IDs and generating SAS URL
        // Similar to the uploadFileAndGenerateSasUrl method, but handling summary file
    }
}
