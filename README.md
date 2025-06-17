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

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    private final RestTemplate restTemplate;

    @Value("${azure.keyvault.url}")
    private String keyVaultUrl;

    @Value("${azure.keyvault.ecm-fm-account-key}")
    private String accountKeySecretName;

    @Value("${azure.keyvault.ecm-fm-account-name}")
    private String accountNameSecretName;

    @Value("${azure.keyvault.ecm-fm-container-name}")
    private String containerNameSecretName;

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

            accountKey = getSecret(secretClient, accountKeySecretName);
            accountName = getSecret(secretClient, accountNameSecretName);
            containerName = getSecret(secretClient, containerNameSecretName);

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

    public String copyFileFromUrlToBlob(String sourceUrl, String targetFolder, KafkaMessage message) {
        initSecrets();
        try {
            logger.info("📥 Downloading file from URL: {}", sourceUrl);
            byte[] content = downloadContentFromUrl(sourceUrl);
            String fileName = Paths.get(new URI(sourceUrl).getPath()).getFileName().toString();
            String blobName = buildBlobPath(targetFolder, message, fileName);

            BlobServiceClient serviceClient = getBlobServiceClient();
            BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            logger.info("📤 Uploading file to blob: {}", blobName);
            blobClient.upload(new ByteArrayInputStream(content), content.length, true);

            String blobUrl = blobClient.getBlobUrl();
            logger.info("✅ Uploaded blob URL: {}", blobUrl);
            return blobUrl;
        } catch (Exception e) {
            logger.error("❌ Failed to copy file to blob: {}", e.getMessage(), e);
            throw new CustomAppException("Failed to copy file to blob", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String uploadFile(String folderPath, String fileName, byte[] content) {
        initSecrets();
        try {
            String blobName = folderPath + "/" + fileName;
            BlobServiceClient serviceClient = getBlobServiceClient();
            BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            blobClient.upload(new ByteArrayInputStream(content), content.length, true);

            String blobUrl = blobClient.getBlobUrl();
            logger.info("✅ Uploaded file URL: {}", blobUrl);
            return blobUrl;
        } catch (Exception e) {
            logger.error("❌ Failed to upload file: {}", e.getMessage(), e);
            throw new CustomAppException("Failed to upload file", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String downloadFileContent(String blobUrl) {
        initSecrets();
        try {
            logger.info("📥 Downloading content from blob URL: {}", blobUrl);
            BlobServiceClient serviceClient = getBlobServiceClient();
            URI uri = new URI(blobUrl);
            String path = uri.getPath();
            String blobName = Arrays.stream(path.split("/")).skip(2).reduce((first, second) -> second).orElse("");
            BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            blobClient.download(outputStream);
            return outputStream.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.error("❌ Failed to download content from blob: {}", e.getMessage(), e);
            throw new CustomAppException("Failed to download blob content", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String buildPrintFileUrl(String folderPath, String printFileName) {
        initSecrets();
        try {
            BlobServiceClient serviceClient = getBlobServiceClient();
            BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(folderPath + "/" + printFileName);

            if (blobClient.exists()) {
                String blobUrl = blobClient.getBlobUrl();
                logger.info("✅ Print file exists, URL: {}", blobUrl);
                return blobUrl;
            } else {
                logger.warn("⚠️ Print file does not exist: {}", folderPath + "/" + printFileName);
                return null;
            }
        } catch (Exception e) {
            logger.error("❌ Failed to build print file URL: {}", e.getMessage(), e);
            throw new CustomAppException("Failed to build print file URL", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String uploadSummaryJson(String folderPath, String summaryFileName, String jsonContent) {
        initSecrets();
        try {
            byte[] content = jsonContent.getBytes(StandardCharsets.UTF_8);
            String blobName = folderPath + "/" + summaryFileName;

            BlobServiceClient serviceClient = getBlobServiceClient();
            BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            blobClient.upload(new ByteArrayInputStream(content), content.length, true);

            String blobUrl = blobClient.getBlobUrl();
            logger.info("✅ Uploaded summary JSON URL: {}", blobUrl);
            return blobUrl;
        } catch (Exception e) {
            logger.error("❌ Failed to upload summary JSON: {}", e.getMessage(), e);
            throw new CustomAppException("Failed to upload summary JSON", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private byte[] downloadContentFromUrl(String fileUrl) throws IOException {
        URL url = new URL(fileUrl);
        try (InputStream in = url.openStream()) {
            return in.readAllBytes();
        }
    }

    private BlobServiceClient getBlobServiceClient() {
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        return new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(credential)
                .buildClient();
    }

    private String buildBlobPath(String targetFolder, KafkaMessage message, String fileName) {
        String batchId = message.getBatchID() != null ? message.getBatchID() : "unknown_batch";
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                .withZone(ZoneId.systemDefault())
                .format(Instant.now());
        return String.format("%s/%s/%s_%s", targetFolder, batchId, timestamp, fileName);
    }
}

azure.keyvault.url=https://nsn-dev-ecm-kva-001.vault.azure.net
azure.keyvault.ecm-fm-account-key=ecm-fm-account-key
azure.keyvault.ecm-fm-account-name=ecm-fm-account-name
azure.keyvault.ecm-fm-container-name=ecm-fm-container-name
azure.blob.storage.format=https://%s.blob.core.windows.net/%s
