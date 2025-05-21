package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
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
import java.net.SocketTimeoutException;
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

    @Value("${use.proxy:false}")
    private boolean useProxy;

    public BlobStorageService(RestTemplate restTemplate, ProxySetup proxySetup) {
        this.restTemplate = restTemplate;
        this.proxySetup = proxySetup;
    }

    public String uploadFileAndGenerateSasUrl(String fileLocation, String batchId, String objectId) {
        if (fileLocation == null || batchId == null || objectId == null) {
            logger.error("Bad Request: One or more input parameters are null.");
            throw new BlobStorageException("400 - Bad Request: Null input detected.", null, null);
        }

        try {
            String accountKey = ""; // Placeholder for actual Vault secret
            String accountName = "nsndvextr01";
            String containerName = "nsnakscontregecm001";

            String extension = getFileExtension(fileLocation);
            if (extension.isEmpty()) {
                logger.error("Unable to determine file extension from: {}", fileLocation);
                throw new BlobStorageException("400 - Invalid file extension.", null, null);
            }

            String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

            String sourceBlobName = fileLocation.substring(fileLocation.lastIndexOf("/") + 1);
            BlobClient sourceBlobClient = containerClient.getBlobClient(sourceBlobName);

            try (InputStream inputStream = sourceBlobClient.openInputStream()) {
                long sourceSize = sourceBlobClient.getProperties().getBlobSize();
                targetBlobClient.upload(inputStream, sourceSize, true);
                logger.info("✅ File uploaded: '{}' -> '{}'", sourceBlobName, targetBlobClient.getBlobUrl());
            } catch (SocketTimeoutException ste) {
                logger.error("420 - Connection timeout: {}", ste.getMessage(), ste);
                throw new BlobStorageException("420 - Connection timeout.", ste, null);
            } catch (IOException ioe) {
                logger.error("601 - Local I/O error: {}", ioe.getMessage(), ioe);
                throw new BlobStorageException("601 - Local I/O error.", ioe, null);
            } catch (Exception ex) {
                logger.error("453 - Upload failed: {}", ex.getMessage(), ex);
                throw new BlobStorageException("453 - Upload failed.", ex, null);
            }

            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                    OffsetDateTime.now().plusHours(24),
                    new BlobSasPermission().setReadPermission(true)
            );

            String sasToken = targetBlobClient.generateSas(sasValues);
            String sasUrl = targetBlobClient.getBlobUrl() + "?" + sasToken;
            logger.info("🔐 SAS URL generated: {}", sasUrl);
            return sasUrl;

        } catch (BlobStorageException e) {
            logger.error("Blob storage error: {}", e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            if (e.getMessage().contains("SSL")) {
                logger.error("530 - SSL error: {}", e.getMessage(), e);
                throw new BlobStorageException("530 - SSL Exception.", e, null);
            }
            logger.error("451 - Runtime error: {}", e.getMessage(), e);
            throw new BlobStorageException("451 - Unexpected runtime error.", e, null);
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
            logger.error("401 - Unauthorized Vault access: {}", e.getMessage());
            throw new BlobStorageException("401 - Unauthorized Vault access.", e, null);
        }
    }

    private String getSecretFromVault(String key, String token) {
        try {
            String url = VAULT_URL + "/v1/Store_Dev/10099";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-vault-namespace", VAULT_NAMESPACE);
            headers.set("x-vault-token", token);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(new HashMap<>(), headers);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, request, String.class);

            JSONObject json = new JSONObject(response.getBody());
            return json.getJSONObject("data").getString(key);

        } catch (Exception e) {
            logger.error("403 - Forbidden Vault access: {}", e.getMessage());
            throw new BlobStorageException("403 - Forbidden Vault access.", e, null);
        }
    }

    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        return lastDotIndex > 0 ? fileLocation.substring(lastDotIndex) : "";
    }
}
