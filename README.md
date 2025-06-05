blob = nsndvextr01

container = nsndvextr01

KV = https://nsn-dev-ecm-kva-001.vault.azure.net/

PV & PVC exists on dev-exstream, re-use the same for blob


package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.common.StorageSharedKeyCredential;
//import com.nedbank.kafka.filemanage.config.ProxySetup;
import com.nedbank.kafka.filemanage.exception.CustomAppException;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    private final RestTemplate restTemplate;
    //private final ProxySetup proxySetup;

    @Value("${vault.hashicorp.url}")
    private String VAULT_URL;

    @Value("${vault.hashicorp.namespace}")
    private String VAULT_NAMESPACE;

    @Value("${vault.hashicorp.passwordDev}")
    private String passwordDev;

    @Value("${vault.hashicorp.passwordNbhDev}")
    private String passwordNbhDev;

    @Value("${use.proxy:false}")
    private boolean useProxy;

    private String accountKey;
    private String accountName;
    private String containerName;

    /*public BlobStorageService(RestTemplate restTemplate, ProxySetup proxySetup) {
        this.restTemplate = restTemplate;
        this.proxySetup = proxySetup;
    }*/

    public BlobStorageService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) {
            return; // already initialized
        }
        /*String token = getVaultToken();
        accountKey = getSecretFromVault("account_key", token);
        accountName = getSecretFromVault("account_name", token);
        containerName = getSecretFromVault("container_name", token);*/
        accountKey = "";
        accountName = "nsndvextr01";//getSecretFromVault("account_name", token);
        containerName = "nsnakscontregecm001";//getSecretFromVault("container_name", token);
        logger.info("Vault secrets initialized for Blob Storage");
    }

    public String copyFileFromUrlToBlob(String sourceUrl, String targetBlobPath) {
        try {
            initSecrets();

            // Parse sourceUrl to get source account, container, blob path
            URI sourceUri = new URI(sourceUrl);
            String host = sourceUri.getHost(); // e.g. nsndvextr01.blob.core.windows.net
            String[] hostParts = host.split("\\.");
            String sourceAccountName = hostParts[0]; // e.g. nsndvextr01

            String path = sourceUri.getPath(); // e.g. /nsnakscontregecm001/DEBTMAN.csv
            String[] pathParts = path.split("/", 3);
            if (pathParts.length < 3) {
                throw new CustomAppException("Invalid source URL path: " + path, 400, HttpStatus.BAD_REQUEST);
            }
            String sourceContainerName = pathParts[1];
            String sourceBlobPath = pathParts[2];

            // Create source BlobClient
            BlobServiceClient sourceBlobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", sourceAccountName))
                    .credential(new StorageSharedKeyCredential(sourceAccountName, accountKey))
                    .buildClient();

            BlobContainerClient sourceContainerClient = sourceBlobServiceClient.getBlobContainerClient(sourceContainerName);
            BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(sourceBlobPath);

            // Fallback: check blob size using listBlobs() (avoid BlobProperties)
            long blobSize = -1;
            for (BlobItem item : sourceContainerClient.listBlobs()) {
                if (item.getName().equals(sourceBlobPath)) {
                    blobSize = item.getProperties().getContentLength();
                    break;
                }
            }

            if (blobSize == -1) {
                throw new CustomAppException("Blob not found: " + sourceBlobPath, 404, HttpStatus.NOT_FOUND);
            }

            logger.info("📄 Source blob '{}' size: {} bytes", sourceUrl, blobSize);

            if (blobSize == 0) {
                logger.warn("⚠️ Source blob '{}' is empty. Skipping copy.", sourceUrl);
                throw new CustomAppException("Source blob is empty: " + sourceUrl, 400, HttpStatus.BAD_REQUEST);
            }

            // Download and prepare input stream
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            sourceBlobClient.download(outputStream);
            byte[] sourceBlobBytes = outputStream.toByteArray();
            InputStream inputStream = new ByteArrayInputStream(sourceBlobBytes);

            // Target blob client
            BlobServiceClient targetBlobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient targetContainerClient = targetBlobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = targetContainerClient.getBlobClient(targetBlobPath);

            targetBlobClient.upload(inputStream, sourceBlobBytes.length, true);

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
            logger.error("❌ Vault token fetch failed: {}", e.getMessage());
            throw new CustomAppException("Vault authentication error", 401, HttpStatus.UNAUTHORIZED, e);
        }
    }

    private String getSecretFromVault(String key, String token) {
        try {
            String url = VAULT_URL + "/v1/Store_Dev/10099";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-vault-namespace", VAULT_NAMESPACE);
            headers.set("x-vault-token", token);

            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(headers), String.class);

            JSONObject json = new JSONObject(Objects.requireNonNull(response.getBody()));
            return json.getJSONObject("data").getString(key);
        } catch (Exception e) {
            logger.error("❌ Failed to retrieve secret from Vault: {}", e.getMessage());
            throw new CustomAppException("Vault secret fetch error", 403, HttpStatus.FORBIDDEN, e);
        }
    }

    public String downloadFileContent(String blobPathOrUrl) {
        try {
            initSecrets();

            String extractedContainerName = containerName;  // default if container name is configured
            String blobName = blobPathOrUrl;

            // Handle full blob URL
            if (blobPathOrUrl.startsWith("http")) {
                URI uri = new URI(blobPathOrUrl);
                String[] segments = uri.getPath().split("/");

                if (segments.length < 3) {
                    throw new CustomAppException("Invalid blob URL format: " + blobPathOrUrl, 400, HttpStatus.BAD_REQUEST);
                }

                // Example: /container/blob => segments[1] = container, segments[2...] = blob path
                extractedContainerName = segments[1];
                blobName = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
            }

            logger.info("🔍 Downloading blob: container='{}', blob='{}'", extractedContainerName, blobName);

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

        String baseUrl = String.format("https://%s.blob.core.windows.net/%s", accountName, containerName);

        // Format date folder from message timestamp
        String dateFolder = Instant.ofEpochMilli(message.getTimestamp())
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        // Compose print file name or use message info as needed
        String printFileName = message.getBatchId() + "_printfile.pdf";

        // Construct full URL path (adjust folders if needed)
        String printFileUrl = String.format("%s/%s/%s/%s/%s/%s/print/%s",
                baseUrl,
                message.getSourceSystem(),
                dateFolder,
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                message.getJobName(),
                printFileName);

        logger.info("Built print file URL: {}", printFileUrl);
        return printFileUrl;
    }

    /**
     * Uploads the summary JSON file and returns the Blob URL.
     *
     * @param localFilePathOrUrl Local path of the summary JSON file.
     * @param message       KafkaMessage object containing metadata to construct remote path.
     * @return URL of the uploaded summary JSON file.
     */
    public String uploadSummaryJson(String localFilePathOrUrl, KafkaMessage message) {
        initSecrets();

        String remoteBlobPath = String.format("%s/%s/%s/summary.json",
                message.getSourceSystem(),
                message.getBatchId(),
                message.getUniqueConsumerRef());

        String jsonContent;

        try {
            if (localFilePathOrUrl.startsWith("http://") || localFilePathOrUrl.startsWith("https://")) {
                // It's a URL, download content via HTTP
                jsonContent = downloadContentFromUrl(localFilePathOrUrl);
            } else {
                // It's a local file path, read from file system
                jsonContent = Files.readString(Paths.get(localFilePathOrUrl));
            }
        } catch (Exception e) {
            logger.error("Error reading summary JSON content at {}: {}", localFilePathOrUrl, e.getMessage(), e);
            throw new CustomAppException("Error reading summary JSON content", 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }

        // Upload JSON content to Azure Blob Storage
        String uploadedUrl = uploadFile(jsonContent, remoteBlobPath);
        logger.info("Uploaded summary JSON to '{}'", uploadedUrl);

        return uploadedUrl;
    }

    // Helper method to download content from URL
    private String downloadContentFromUrl(String urlString) throws IOException {
        try (InputStream in = new URL(urlString).openStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

}
