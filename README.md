package com.nedbank.kafka.filemanage.service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.constants.BlobStorageConstants;
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

/**
 * Service for interacting with Azure Blob Storage.
 * Handles uploading and downloading files and content,
 * fetching secrets from Azure Key Vault, and constructing blob paths based on KafkaMessage metadata.
 *
 * Uploads include text, binary, file path-based, and summary JSONs.
 * Downloads stream content directly to local file system to minimize memory usage.
 *
 * Dependencies: Azure Blob Storage SDK, Azure Key Vault SDK.
 */
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

    /**
     * Lazily initializes and caches secrets required for Azure Blob operations.
     *
     * <p>This method ensures that secrets such as {@code accountName}, {@code accountKey}, and
     * {@code containerName} are fetched from Azure Key Vault only when needed.</p>
     *
     * <p>To avoid repeated Key Vault calls, the secrets are cached and refreshed based on a
     * configurable TTL (time-to-live). If the TTL has not expired since the last refresh, the
     * cached values are reused.</p>
     *
     * <p>This design balances performance with sensitivity to secret updates in Key Vault.</p>
     */
    private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) {
            if (lastSecretRefreshTime != null &&
                    Instant.now().toEpochMilli() - lastSecretRefreshTime.toEpochMilli() < BlobStorageConstants.SECRET_CACHE_TTL_MS) {
                return;
            }
        }

        try {
            logger.info("🔐 Fetching secrets from Azure Key Vault...");
            logger.info("📌 Key Vault URL          : {}", keyVaultUrl);
            logger.info("📌 Secret Names Requested : {}, {}, {}",
                    fmAccountKey, fmAccountName, fmContainerName);

            SecretClient secretClient = new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();

            accountKey = getSecret(secretClient, fmAccountKey);
            accountName = getSecret(secretClient, fmAccountName);
            containerName = getSecret(secretClient, fmContainerName);

            logger.info("📦 Azure Storage Secrets fetched:");
            logger.info("   • Account Key    : {}", accountKey);
            logger.info("   • Account Name   : {}", accountName);
            logger.info("   • Container Name : {}", containerName);

            if (accountKey == null || accountName == null || containerName == null) {
                throw new CustomAppException(BlobStorageConstants.ERR_MISSING_SECRETS, 400, HttpStatus.BAD_REQUEST);
            }

            lastSecretRefreshTime = Instant.now();
            logger.info("✅ Secrets fetched successfully from Key Vault.");
        } catch (Exception e) {
            logger.error("❌ Failed to initialize secrets: {}", e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_KV_FAILURE, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }

    }

    @Scheduled(fixedDelay = BlobStorageConstants.SECRET_CACHE_TTL_MS)
    public void scheduledSecretRefresh() {
        logger.info("🕒 Scheduled refresh of secrets...");
        initSecrets();
    }

    /**
     * Fetches a specific secret from Azure Key Vault.
     *
     * @param client     The initialized SecretClient.
     * @param secretName The name of the secret to retrieve.
     * @return The secret value.
     */
    private String getSecret(SecretClient client, String secretName) {
        try {
            return client.getSecret(secretName).getValue();
        } catch (Exception e) {
            logger.error("❌ Failed to fetch secret '{}': {}", secretName, e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_FETCH_SECRET + secretName, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
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
                throw new IllegalArgumentException(BlobStorageConstants.ERR_UNSUPPORTED_TYPE + input.getClass());
            }

            return uploadToBlobStorage(content, targetPath, fileName);
        } catch (IOException e) {
            logger.error("❌ Failed to read input for upload: {}", e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_PROCESS_UPLOAD, 606, HttpStatus.INTERNAL_SERVER_ERROR, e);
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
            throw new CustomAppException(BlobStorageConstants.ERR_UPLOAD_FAIL, 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String resolveMimeType(String fileName, byte[] content) {
        try (InputStream is = new ByteArrayInputStream(content)) {
            Tika tika = new Tika();
            String mimeType = tika.detect(is, fileName);
            return mimeType != null ? mimeType : BlobStorageConstants.DEFAULT_MIME;
        } catch (IOException e) {
            logger.warn("⚠️ Tika failed to detect MIME type. Defaulting. Error: {}", e.getMessage());
            return BlobStorageConstants.DEFAULT_MIME;
        }
    }

    /**
     * Uploads a file based on KafkaMessage context to a constructed blob path.
     *
     * @param file       The file to upload.
     * @param folderName The target subfolder in blob.
     * @param msg        The Kafka message for metadata.
     * @return The uploaded blob URL.
     */
    public String uploadFileByMessage(File file, String folderName, KafkaMessage msg) {
        try {
            byte[] content = Files.readAllBytes(file.toPath());
            String targetPath = buildBlobPath(file.getName(), folderName, msg);
            return uploadFile(content, targetPath);
        } catch (IOException e) {
            logger.error("❌ Error reading file for upload: {}", file.getAbsolutePath(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_FILE_READ, 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    /**
     * Constructs a blob path using file name and KafkaMessage fields.
     *
     * @param fileName   The name of the file.
     * @param folderName Folder name to be included in path.
     * @param msg        Kafka message with metadata.
     * @return Formatted blob path.
     */
    private String buildBlobPath(String fileName, String folderName, KafkaMessage msg) {
        String sourceSystem = sanitize(msg.getSourceSystem(), BlobStorageConstants.FALLBACK_SOURCE);
        String consumerRef = sanitize(msg.getUniqueConsumerRef(), BlobStorageConstants.FALLBACK_CONSUMER);

        return sourceSystem + "/" +
                consumerRef + "/" +
                folderName + "/" +
                fileName;
    }

    private String sanitize(String value, String fallback) {
        if (value == null || value.trim().isEmpty()) return fallback;
        return value.replaceAll(BlobStorageConstants.SANITIZE_REGEX, BlobStorageConstants.SANITIZE_REPLACEMENT);
    }

    /**
     * Downloads a file from blob storage to a local path using streaming.
     *
     * @param blobUrl       Blob URL of the file.
     * @param localFilePath Local path where file will be stored.
     * @return Path to the downloaded file.
     */
    public Path downloadFileToLocal(String blobUrl, Path localFilePath) {
        try {
            initSecrets();
            String container = containerName;
            String blobPath = blobUrl;

            if (blobUrl.startsWith("http")) {
                URI uri = new URI(blobUrl);
                String[] segments = uri.getPath().split("/");
                if (segments.length < 3)
                    throw new CustomAppException(BlobStorageConstants.ERR_INVALID_URL, 400, HttpStatus.BAD_REQUEST);
                container = segments[1];
                blobPath = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
            }

            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(container).getBlobClient(blobPath);
            if (!blob.exists())
                throw new CustomAppException(BlobStorageConstants.ERR_BLOB_NOT_FOUND, 404, HttpStatus.NOT_FOUND);

            try (OutputStream outputStream = new FileOutputStream(localFilePath.toFile())) {
                blob.download(outputStream);
            }

            return localFilePath;

        } catch (Exception e) {
            logger.error("❌ Download to local failed: {}", e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_DOWNLOAD_FAIL, 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    /**
     * Uploads summary.json from either a file path or URL into a target blob path.
     *
     * @param filePathOrUrl File path or URL of summary JSON.
     * @param message       KafkaMessage used to construct remote blob path.
     * @param fileName      Target file name in blob.
     * @return Uploaded blob URL.
     */
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
            throw new CustomAppException(BlobStorageConstants.ERR_SUMMARY_JSON, 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}
