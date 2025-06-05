package com.nedbank.kafka.filemanage.service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.exception.CustomAppException;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Objects;

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    private final RestTemplate restTemplate;

    @Value("${azure.keyvault.url}")
    private String keyVaultUrl;

    private String accountKey;
    private String accountName;
    private String containerName;

    public BlobStorageService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) {
            return;
        }

        try {
            logger.info("🔐 Fetching secrets from Azure Key Vault...");
            SecretClient secretClient = new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();

            accountKey = getSecret(secretClient, "account-key");
            accountName = getSecret(secretClient, "account-name");
            containerName = getSecret(secretClient, "container-name");

            if (accountKey == null || accountKey.isBlank() ||
                accountName == null || accountName.isBlank() ||
                containerName == null || containerName.isBlank()) {
                throw new CustomAppException("One or more secrets are null/empty from Key Vault", 400, HttpStatus.BAD_REQUEST);
            }

            logger.info("✅ Secrets fetched successfully from Azure Key Vault.");
        } catch (Exception e) {
            logger.error("❌ Failed to initialize secrets from Key Vault: {}", e.getMessage(), e);
            throw new CustomAppException("Key Vault integration failure", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String getSecret(SecretClient client, String secretName) {
        try {
            KeyVaultSecret secret = client.getSecret(secretName);
            return secret.getValue();
        } catch (Exception e) {
            logger.error("❌ Failed to fetch secret '{}': {}", secretName, e.getMessage(), e);
            throw new CustomAppException("Failed to fetch secret: " + secretName, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String copyFileFromUrlToBlob(String sourceUrl, String targetBlobPath) {
        try {
            initSecrets();

            URI sourceUri = new URI(sourceUrl);
            String host = sourceUri.getHost();
            String[] hostParts = host.split("\\.");
            String sourceAccountName = hostParts[0];

            String path = sourceUri.getPath();
            String[] pathParts = path.split("/", 3);
            if (pathParts.length < 3) {
                throw new CustomAppException("Invalid source URL path: " + path, 400, HttpStatus.BAD_REQUEST);
            }

            String sourceContainerName = pathParts[1];
            String sourceBlobPath = pathParts[2];

            BlobServiceClient sourceBlobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", sourceAccountName))
                    .credential(new StorageSharedKeyCredential(sourceAccountName, accountKey))
                    .buildClient();

            BlobContainerClient sourceContainerClient = sourceBlobServiceClient.getBlobContainerClient(sourceContainerName);
            BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(sourceBlobPath);

            long blobSize = -1;
            for (BlobItem item : sourceContainerClient.listBlobs()) {
                if (item.getName().equals(sourceBlobPath)) {
                    blobSize = item.getProperties().getContentLength();
                    break;
                }
            }

            if (blobSize <= 0) {
                throw new CustomAppException("Source blob is empty or not found: " + sourceUrl, 404, HttpStatus.NOT_FOUND);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            sourceBlobClient.download(outputStream);
            byte[] sourceBlobBytes = outputStream.toByteArray();

            BlobServiceClient targetBlobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient targetContainerClient = targetBlobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = targetContainerClient.getBlobClient(targetBlobPath);

            targetBlobClient.upload(new ByteArrayInputStream(sourceBlobBytes), sourceBlobBytes.length, true);

            logger.info("✅ Copied '{}' to '{}'", sourceUrl, targetBlobClient.getBlobUrl());

            return targetBlobClient.getBlobUrl();

        } catch (Exception e) {
            logger.error("❌ Error copying file from URL: {}", e.getMessage(), e);
            throw new CustomAppException("Error copying file from URL", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String uploadFile(String content, String targetBlobPath) {
        try {
            initSecrets();

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(targetBlobPath);

            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            blobClient.upload(new ByteArrayInputStream(bytes), bytes.length, true);

            logger.info("✅ Uploaded file to '{}'", blobClient.getBlobUrl());

            return blobClient.getBlobUrl();

        } catch (Exception e) {
            logger.error("❌ Error uploading file: {}", e.getMessage(), e);
            throw new CustomAppException("Error uploading file", 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String downloadFileContent(String blobPathOrUrl) {
        try {
            initSecrets();

            String extractedContainerName = containerName;
            String blobName = blobPathOrUrl;

            if (blobPathOrUrl.startsWith("http")) {
                URI uri = new URI(blobPathOrUrl);
                String[] segments = uri.getPath().split("/");

                if (segments == null || segments.length < 3) {
                    throw new CustomAppException("Invalid blob URL format: " + blobPathOrUrl, 400, HttpStatus.BAD_REQUEST);
                }

                extractedContainerName = segments[1];
                blobName = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
            }

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(extractedContainerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            if (!blobClient.exists()) {
                throw new CustomAppException("Blob not found: " + blobName, 404, HttpStatus.NOT_FOUND);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            blobClient.download(outputStream);

            return outputStream.toString(StandardCharsets.UTF_8.name());

        } catch (Exception e) {
            logger.error("❌ Error downloading blob content for '{}': {}", blobPathOrUrl, e.getMessage(), e);
            throw new CustomAppException("Error downloading blob content", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String buildPrintFileUrl(KafkaMessage message) {
        initSecrets();

        if (message == null || message.getBatchId() == null || message.getSourceSystem() == null ||
            message.getUniqueConsumerRef() == null || message.getJobName() == null) {
            throw new CustomAppException("Invalid Kafka message data for building print file URL", 400, HttpStatus.BAD_REQUEST);
        }

        String baseUrl = String.format("https://%s.blob.core.windows.net/%s", accountName, containerName);

        String dateFolder = Instant.ofEpochMilli(message.getTimestamp())
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        String printFileName = message.getBatchId() + "_printfile.pdf";

        return String.format("%s/%s/%s/%s/%s/%s/print/%s",
                baseUrl,
                message.getSourceSystem(),
                dateFolder,
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                message.getJobName(),
                printFileName);
    }

    public String uploadSummaryJson(String localFilePathOrUrl, KafkaMessage message) {
        initSecrets();

        if (message == null || message.getBatchId() == null ||
            message.getSourceSystem() == null || message.getUniqueConsumerRef() == null) {
            throw new CustomAppException("Missing Kafka message metadata for uploading summary JSON", 400, HttpStatus.BAD_REQUEST);
        }

        String remoteBlobPath = String.format("%s/%s/%s/summary.json",
                message.getSourceSystem(),
                message.getBatchId(),
                message.getUniqueConsumerRef());

        String jsonContent;
        try {
            if (localFilePathOrUrl.startsWith("http://") || localFilePathOrUrl.startsWith("https://")) {
                jsonContent = downloadContentFromUrl(localFilePathOrUrl);
            } else {
                jsonContent = Files.readString(Paths.get(localFilePathOrUrl));
            }
        } catch (Exception e) {
            logger.error("❌ Error reading summary JSON content: {}", e.getMessage(), e);
            throw new CustomAppException("Error reading summary JSON content", 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }

        return uploadFile(jsonContent, remoteBlobPath);
    }

    private String downloadContentFromUrl(String urlString) throws IOException {
        try (InputStream in = new URL(urlString).openStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
