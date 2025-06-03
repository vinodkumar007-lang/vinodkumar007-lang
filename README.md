package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.common.StorageSharedKeyCredential;
//import com.nedbank.kafka.filemanage.config.ProxySetup;
import com.nedbank.kafka.filemanage.exception.CustomAppException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
            blobClient.upload(new java.io.ByteArrayInputStream(bytes), bytes.length, true);

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

}
