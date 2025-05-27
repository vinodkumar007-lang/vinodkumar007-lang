package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
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

    private String accountKey;
    private String accountName;
    private String containerName;

    public BlobStorageService(RestTemplate restTemplate, ProxySetup proxySetup) {
        this.restTemplate = restTemplate;
        this.proxySetup = proxySetup;
    }

    private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) {
            return; // already initialized
        }
        String token = getVaultToken();
        accountKey = getSecretFromVault("account_key", token);
        accountName = getSecretFromVault("account_name", token);
        containerName = getSecretFromVault("container_name", token);
        logger.info("Vault secrets initialized for Blob Storage");
    }

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

            initSecrets();

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

            try (InputStream inputStream = restTemplate.getForObject(sourceUrl, InputStream.class)) {
                if (inputStream == null) {
                    throw new CustomAppException("Unable to read source file from URL: " + sourceUrl, 404, HttpStatus.NOT_FOUND);
                }

                byte[] data = inputStream.readAllBytes();

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

    public String uploadSummaryJson(
            String sourceSystem,
            String batchId,
            String timestamp,
            String summaryJsonContent) {

        try {
            if (summaryJsonContent == null || sourceSystem == null || batchId == null || timestamp == null) {
                throw new CustomAppException("Required parameters missing for summary upload", 400, HttpStatus.BAD_REQUEST);
            }

            initSecrets();

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
