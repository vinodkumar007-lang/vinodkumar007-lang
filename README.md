package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.config.ProxySetup;
import com.nedbank.kafka.filemanage.exception.CustomAppException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.InputStream;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
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

    @Value("${use.proxy:false}")
    private boolean useProxy;

    public BlobStorageService(RestTemplate restTemplate, ProxySetup proxySetup) {
        this.restTemplate = restTemplate;
        this.proxySetup = proxySetup;
    }

    /**
     * Copies a file from an external URL (e.g., blobUrl from Kafka message)
     * into Azure Blob Storage with the folder structure:
     * {sourceSystem}/input/{timestamp}/{batchId}/{consumerReference}_{processReference}/{fileName}
     *
     * @return The URL of the copied file in Azure Blob Storage
     */
    public String copyFileFromUrlToBlob(
            String sourceUrl,
            String sourceSystem,
            String batchId,
            String consumerReference,
            String processReference,
            String timestamp,
            String fileName) {

        try {
            if (sourceUrl == null || sourceSystem == null || batchId == null
                    || consumerReference == null || processReference == null
                    || timestamp == null || fileName == null) {
                throw new CustomAppException("Required parameters missing", 400, HttpStatus.BAD_REQUEST);
            }

            // TODO: Replace these with secrets fetched from Vault
            String accountKey = ""; // getSecretFromVault("account_key", getVaultToken());
            String accountName = "nsndvextr01"; // getSecretFromVault("account_name", getVaultToken());
            String containerName = "nsnakscontregecm001"; // getSecretFromVault("container_name", getVaultToken());

            // Build target blob path with folder structure
            String blobName = String.format("%s/input/%s/%s/%s_%s/%s",
                    sourceSystem,
                    timestamp,
                    batchId,
                    consumerReference.replaceAll("[{}]", ""),
                    processReference.replaceAll("[{}]", ""),
                    fileName);

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

            // Download the file from the sourceUrl via RestTemplate
            try (InputStream inputStream = restTemplate.getForObject(sourceUrl, InputStream.class)) {
                if (inputStream == null) {
                    throw new CustomAppException("Unable to read source file from URL: " + sourceUrl, 404, HttpStatus.NOT_FOUND);
                }

                // Read all bytes to get size - caution: for large files, consider streaming alternative
                byte[] data = inputStream.readAllBytes();

                // Upload to target blob (overwrite if exists)
                targetBlobClient.upload(new java.io.ByteArrayInputStream(data), data.length, true);

                logger.info("✅ Copied '{}' to '{}'", sourceUrl, targetBlobClient.getBlobUrl());
            }

            return targetBlobClient.getBlobUrl();

        } catch (CustomAppException cae) {
            throw cae;
        } catch (Exception e) {
            logger.error("❌ Error copying file from URL: {}", e.getMessage(), e);
            throw new CustomAppException("Error copying file from URL", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    /**
     * Uploads the summary.json content to Azure Blob Storage with folder structure:
     * {sourceSystem}/summary/{timestamp}/{batchId}/summary.json
     *
     * @return The URL of the uploaded summary.json file
     */
    public String uploadSummaryJson(
            String sourceSystem,
            String batchId,
            String timestamp,
            String summaryJsonContent) {

        try {
            if (summaryJsonContent == null || sourceSystem == null || batchId == null || timestamp == null) {
                throw new CustomAppException("Required parameters missing for summary upload", 400, HttpStatus.BAD_REQUEST);
            }

            // TODO: Replace these with secrets fetched from Vault
            String accountKey = "";
            String accountName = "nsndvextr01";
            String containerName = "nsnakscontregecm001";

            String blobName = String.format("%s/summary/%s/%s/summary.json",
                    sourceSystem,
                    timestamp,
                    batchId);

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient summaryBlobClient = containerClient.getBlobClient(blobName);

            byte[] jsonBytes = summaryJsonContent.getBytes(StandardCharsets.UTF_8);

            summaryBlobClient.upload(new java.io.ByteArrayInputStream(jsonBytes), jsonBytes.length, true);

            logger.info("✅ Uploaded summary.json to '{}'", summaryBlobClient.getBlobUrl());

            return summaryBlobClient.getBlobUrl();

        } catch (CustomAppException cae) {
            throw cae;
        } catch (Exception e) {
            logger.error("❌ Error uploading summary.json: {}", e.getMessage(), e);
            throw new CustomAppException("Error uploading summary.json", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    // Your existing method to upload file inside same container from a fileLocation path
    // (you can keep this if needed; it's not used in above copy logic)
    public String uploadFileAndReturnLocation(
            String sourceSystem,
            String fileLocation,
            String batchId,
            String objectId,
            String consumerReference,
            String processReference,
            String timestamp) {

        try {
            if (sourceSystem == null || fileLocation == null || batchId == null || objectId == null
                    || consumerReference == null || processReference == null || timestamp == null) {
                throw new CustomAppException("Required parameters missing", 400, HttpStatus.BAD_REQUEST);
            }

            // TODO: Replace with Vault secrets
            String accountKey = ""; // getSecretFromVault("account_key", getVaultToken());
            String accountName = "nsndvextr01"; // getSecretFromVault("account_name", getVaultToken());
            String containerName = "nsnakscontregecm001"; // getSecretFromVault("container_name", getVaultToken());

            String sourceFileName = fileLocation.substring(fileLocation.lastIndexOf("/") + 1);

            String blobName = String.format("%s/input/%s/%s/%s_%s/%s",
                    sourceSystem,
                    timestamp,
                    batchId,
                    consumerReference.replaceAll("[{}]", ""),
                    processReference.replaceAll("[{}]", ""),
                    sourceFileName);

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient sourceBlobClient = containerClient.getBlobClient(sourceFileName);
            BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

            try (InputStream inputStream = sourceBlobClient.openInputStream()) {
                long size = sourceBlobClient.getProperties().getBlobSize();
                targetBlobClient.upload(inputStream, size, true);
                logger.info("✅ Uploaded '{}' to '{}'", sourceFileName, targetBlobClient.getBlobUrl());
            } catch (BlobStorageException bse) {
                logger.error("❌ Azure Blob Storage error: {}", bse.getMessage());
                throw new CustomAppException("Blob storage operation failed", 453, HttpStatus.BAD_GATEWAY, bse);
            } catch (SocketException se) {
                logger.error("❌ Network error: {}", se.getMessage());
                throw new CustomAppException("Network issue during blob transfer", 420, HttpStatus.GATEWAY_TIMEOUT, se);
            } catch (Exception e) {
                logger.error("❌ Unexpected error during blob transfer: {}", e.getMessage());
                throw new CustomAppException("Unexpected blob error", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
            }

            return targetBlobClient.getBlobUrl();

        } catch (CustomAppException cae) {
            throw cae;
        } catch (Exception e) {
            logger.error("❌ Generic error in uploadFileAndReturnLocation: {}", e.getMessage());
            throw new CustomAppException("Internal blob error", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    // Vault authentication method (unchanged)
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

    // Vault secret fetch method (unchanged)
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

            JSONObject json = new JSONObject(Objects.requireNonNull(response.getBody()));
            return json.getJSONObject("data").getString(key);
        } catch (Exception e) {
            logger.error("❌ Failed to retrieve secret from Vault: {}", e.getMessage());
            throw new CustomAppException("Vault secret fetch error", 403, HttpStatus.FORBIDDEN, e);
        }
    }

    private String getFileNameFromUrl(String url) {
        if (url == null || url.isEmpty()) return "unknownFile";
        int lastSlashIndex = url.lastIndexOf('/');
        if (lastSlashIndex < 0) return url;
        return url.substring(lastSlashIndex + 1);
    }
}
