package com.nedbank.kafka.filemanage.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/file")
public class FileProcessingController {

    private static final Logger logger = LoggerFactory.getLogger(FileProcessingController.class);
    // Keeping controller for future use or monitoring if needed
    @GetMapping("/health")
    public String healthCheck() {
        logger.info("Health check endpoint hit.");
        return "File Processing Service is up and running.";
    }
}
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

        Map<String, Object> summaryResponse = null;

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

            // Build the summary payload to send to the output Kafka topic
            summaryResponse = buildSummaryPayload(batchId, blobUrl, batchFilesNode);
            String summaryMessage = new ObjectMapper().writeValueAsString(summaryResponse);

            // Send the summary message to the Kafka output topic
            kafkaTemplate.send(outputTopic, batchId, summaryMessage);
            logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

        } catch (Exception e) {
            // Improved error handling with detailed logging
            logger.error("Error processing Kafka message: {}. Error: {}", message, e.getMessage(), e);
        }

        // Return the summary response after processing
        if (summaryResponse != null) {
            logger.info("Returning summary response: {}", summaryResponse);
        } else {
            logger.warn("Summary response is null.");
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

            // Create a dynamic URL that includes both batchId and objectId
            String dynamicFileUrl = blobUrl + "/" + objectId.replaceAll("[{}]", "") + "_" + batchId + "_" + objectId + extension;

            processedFiles.add(new ProcessedFileInfo(objectId, dynamicFileUrl));
        }

        // Create the summary object
        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(batchId);
        summary.setHeader(new HeaderInfo()); // Populate header if required
        summary.setMetadata(new MetadataInfo()); // Populate metadata if required
        summary.setPayload(new PayloadInfo()); // Populate payload if required
        summary.setProcessedFiles(processedFiles);

        // Optionally, include a summary file URL
        summary.setSummaryFileURL(blobUrl + "/summary/" + batchId + "_summary.json");

        // Convert to Map and return
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
package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.config.ProxySetup;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    private final RestTemplate restTemplate;
    private final ProxySetup proxySetup;

    @Value("${vault.hashicorp.url}")
    private String VAULT_URL;

    @Value("${vault.hashicorp.namespace}")
    private String VAULT_NAMESPACE;

    @Value("${vault.hashicorp.passwordDev}")
    private String passwordDev;

    @Value("${vault.hashicorp.passwordNbhDev}")
    private String passwordNbhDev;

    @Value("${use.proxy:false}")  // Flag to enable/disable proxy configuration
    private boolean useProxy;

    public BlobStorageService(RestTemplate restTemplate, ProxySetup proxySetup) {
        this.restTemplate = restTemplate;
        this.proxySetup = proxySetup;
    }

    public String uploadFileAndGenerateSasUrl(String fileLocation, String batchId, String objectId) {
        try {
            // 🔐 Replace with actual Vault logic in production
            String accountKey = ""; // getSecretFromVault("account_key", getVaultToken());
            String accountName = "nsndvextr01"; // getSecretFromVault("account_name", ...);
            String containerName = "nsnakscontregecm001"; // getSecretFromVault("container_name", ...);

            // 📄 Determine file extension and blob name
            String extension = getFileExtension(fileLocation);
            String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            // 📦 Set up Azure Blob client
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

            // 📥 Get source blob name from URL
            String sourceBlobName = fileLocation.substring(fileLocation.lastIndexOf("/") + 1);
            BlobClient sourceBlobClient = containerClient.getBlobClient(sourceBlobName);

            // ⬇️⬆️ Download source and upload to target
            try (InputStream inputStream = sourceBlobClient.openInputStream()) {
                long sourceSize = sourceBlobClient.getProperties().getBlobSize();
                targetBlobClient.upload(inputStream, sourceSize, true); // true = overwrite
                logger.info("✅ File uploaded successfully from '{}' to '{}'", sourceBlobName, targetBlobClient.getBlobUrl());
            } catch (Exception e) {
                logger.error("❌ Error transferring blob: {}", e.getMessage(), e);
                throw new IOException("❌ Failed to transfer blob from source to target", e);
            }

            // 🔗 Generate SAS token with 24-hour read permission
            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                    OffsetDateTime.now().plusHours(24),
                    new BlobSasPermission().setReadPermission(true)
            );

            String sasToken = targetBlobClient.generateSas(sasValues);
            String sasUrl = targetBlobClient.getBlobUrl() + "?" + sasToken;

            logger.info("🔐 SAS URL (valid for 24 hours): {}", sasUrl);
            return sasUrl;

        } catch (IOException e) {
            logger.error("❌ IO error during blob upload or SAS generation: {}", e.getMessage(), e);
            throw new RuntimeException("❌ Error uploading to Azure Blob or generating SAS URL", e);
        } catch (Exception e) {
            logger.error("❌ Unexpected error during blob operation: {}", e.getMessage(), e);
            throw new RuntimeException("❌ Unexpected error in blob upload or SAS URL generation", e);
        }
    }

    private String getVaultToken() {
        try {
            String url = VAULT_URL + "/v1/auth/userpass/login/espire_dev";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-vault-namespace", VAULT_NAMESPACE);

            Map<String, String> body = new HashMap<>();
            body.put("password", passwordDev);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

            JSONObject json = new JSONObject(Objects.requireNonNull(response.getBody()));
            return json.getJSONObject("auth").getString("client_token");
        } catch (Exception e) {
            logger.error("Error getting Vault token: {}", e.getMessage());
            throw new RuntimeException("❌ Failed to obtain Vault token", e);
        }
    }

    private String getSecretFromVault(String key, String token) {
        try {
            String url = VAULT_URL + "/v1/Store_Dev/10099";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-vault-namespace", VAULT_NAMESPACE);
            headers.set("x-vault-token", token);

            Map<String, String> body = new HashMap<>();
            body.put("password", passwordNbhDev);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, request, String.class);

            JSONObject json = new JSONObject(response.getBody());
            return json.getJSONObject("data").getString(key);
        } catch (Exception e) {
            logger.error("Error retrieving secret from Vault: {}", e.getMessage());
            throw new RuntimeException("❌ Failed to retrieve secret from Vault", e);
        }
    }

    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        return lastDotIndex > 0 ? fileLocation.substring(lastDotIndex) : "";
    }
}
