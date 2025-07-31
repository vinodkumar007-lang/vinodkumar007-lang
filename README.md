you have multiple implementation of this template function, doesn't it make more sense to create a private upload method that handles the blob builder and client and you call the same function from the multiple uploads? This code is just duplicate 4 or 5 times in the class.

Consider using Apache Tika (Open source) to determine the mime type from the actual content stream, it is a lot more reliable than using the path extention, use the path name as a default if tika is unable to resolve the mime type

package com.nedbank.kafka.filemanage.service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.exception.CustomAppException;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.Arrays;

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    private final RestTemplate restTemplate;

    @Value("${azure.keyvault.url}")
    private String keyVaultUrl;

    @Value("${azure.blob.storage.format}")
    private String azureStorageFormat;

    @Value("${azure.keyvault.accountKey}")
    private String fmAccountKey;

    @Value("${azure.keyvault.accountName}")
    private String fmAccountName;

    @Value("${azure.keyvault.containerName}")
    private String fmContainerName;

    private String accountKey;
    private String accountName;
    private String containerName;

    private Instant lastSecretRefreshTime = null;
    private static final long SECRET_CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes

    public BlobStorageService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) {
            if (lastSecretRefreshTime != null &&
                Instant.now().toEpochMilli() - lastSecretRefreshTime.toEpochMilli() < SECRET_CACHE_TTL_MS) {
                return;
            }
        }

        try {
            logger.info("🔐 Fetching secrets from Azure Key Vault...");
            SecretClient secretClient = new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();

            accountKey = getSecret(secretClient, fmAccountKey);
            accountName = getSecret(secretClient, fmAccountName);
            containerName = getSecret(secretClient, fmContainerName);

            if (accountKey == null || accountName == null || containerName == null) {
                throw new CustomAppException("Secrets missing from Key Vault", 400, HttpStatus.BAD_REQUEST);
            }

            lastSecretRefreshTime = Instant.now();
            logger.info("✅ Secrets fetched successfully from Key Vault.");
        } catch (Exception e) {
            logger.error("❌ Failed to initialize secrets: {}", e.getMessage(), e);
            throw new CustomAppException("Key Vault integration failure", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    @Scheduled(fixedDelay = SECRET_CACHE_TTL_MS)
    public void scheduledSecretRefresh() {
        logger.info("🕒 Scheduled refresh of secrets...");
        initSecrets();
    }

    private String getSecret(SecretClient client, String secretName) {
        try {
            return client.getSecret(secretName).getValue();
        } catch (Exception e) {
            logger.error("❌ Failed to fetch secret '{}': {}", secretName, e.getMessage(), e);
            throw new CustomAppException("Failed to fetch secret: " + secretName, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    /**
     * Unified method to upload String, byte[], File, or Path to Azure Blob Storage.
     */
    public String uploadFile(Object input, String targetPath) {
        try {
            byte[] content;
            String fileName = Paths.get(targetPath).getFileName().toString();

            if (input instanceof String str) {
                content = str.getBytes(StandardCharsets.UTF_8);
            } else if (input instanceof byte[] bytes) {
                content = bytes;
            } else if (input instanceof File file) {
                content = Files.readAllBytes(file.toPath());
            } else if (input instanceof Path path) {
                content = Files.readAllBytes(path);
            } else {
                throw new IllegalArgumentException("Unsupported input type for upload: " + input.getClass());
            }

            return uploadToBlobStorage(content, targetPath, fileName);
        } catch (IOException e) {
            logger.error("❌ Failed to read input for upload: {}", e.getMessage(), e);
            throw new CustomAppException("Failed to process upload input", 606, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String uploadToBlobStorage(byte[] content, String targetPath, String fileName) {
        try {
            initSecrets();
            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(containerName).getBlobClient(targetPath);

            BlobHttpHeaders headers = new BlobHttpHeaders()
                    .setContentType(resolveMimeType(fileName, content));

            blob.upload(new ByteArrayInputStream(content), content.length, true);
            blob.setHttpHeaders(headers);

            logger.info("📤 Uploaded file to '{}'", blob.getBlobUrl());
            return blob.getBlobUrl();
        } catch (Exception e) {
            logger.error("❌ Upload failed: {}", e.getMessage(), e);
            throw new CustomAppException("Upload failed", 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String resolveMimeType(String fileName, byte[] content) {
        try (InputStream is = new ByteArrayInputStream(content)) {
            Tika tika = new Tika();
            String mimeType = tika.detect(is, fileName);
            return mimeType != null ? mimeType : "application/octet-stream";
        } catch (IOException e) {
            logger.warn("⚠️ Tika failed to detect MIME type. Defaulting. Error: {}", e.getMessage());
            return "application/octet-stream";
        }
    }

    public String uploadFileByMessage(File file, String folderName, KafkaMessage msg) {
        try {
            String targetPath = buildBlobPath(file.getName(), folderName, msg);
            return uploadFile(file, targetPath);
        } catch (Exception e) {
            logger.error("❌ Error in uploadFileByMessage: {}", e.getMessage(), e);
            throw new CustomAppException("File upload by message failed", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String buildBlobPath(String fileName, String folderName, KafkaMessage msg) {
        String sourceSystem = sanitize(msg.getSourceSystem(), "UNKNOWN_SOURCE");
        String consumerRef = sanitize(msg.getUniqueConsumerRef(), "UNKNOWN_CONSUMER");

        return sourceSystem + "/" +
                consumerRef + "/" +
                folderName + "/" +
                fileName;
    }

    private String sanitize(String value, String fallback) {
        if (value == null || value.trim().isEmpty()) return fallback;
        return value.replaceAll("[^a-zA-Z0-9-_]", "_");
    }

    public Path downloadFileToLocal(String blobUrl, Path localFilePath) {
        try {
            initSecrets();
            String container = containerName;
            String blobPath = blobUrl;

            if (blobUrl.startsWith("http")) {
                URI uri = new URI(blobUrl);
                String[] segments = uri.getPath().split("/");
                if (segments.length < 3) throw new CustomAppException("Invalid blob URL", 400, HttpStatus.BAD_REQUEST);
                container = segments[1];
                blobPath = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
            }

            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(container).getBlobClient(blobPath);
            if (!blob.exists()) throw new CustomAppException("Blob not found", 404, HttpStatus.NOT_FOUND);

            try (OutputStream outputStream = new FileOutputStream(localFilePath.toFile())) {
                blob.download(outputStream);
            }

            return localFilePath;
        } catch (Exception e) {
            logger.error("❌ Download to local failed: {}", e.getMessage(), e);
            throw new CustomAppException("Download failed", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String uploadSummaryJson(String filePathOrUrl, KafkaMessage message, String fileName) {
        initSecrets();
        String remotePath = String.format("%s/%s/%s/%s",
                message.getSourceSystem(),
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                fileName);

        try {
            String json = filePathOrUrl.startsWith("http")
                    ? new String(new URL(filePathOrUrl).openStream().readAllBytes(), StandardCharsets.UTF_8)
                    : Files.readString(Paths.get(filePathOrUrl));

            return uploadFile(json, remotePath);
        } catch (Exception e) {
            logger.error("❌ Failed reading summary JSON: {}", e.getMessage(), e);
            throw new CustomAppException("Failed reading summary JSON", 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}

<dependency>
    <groupId>org.apache.tika</groupId>
    <artifactId>tika-core</artifactId>
    <version>2.9.0</version>
</dependency>
