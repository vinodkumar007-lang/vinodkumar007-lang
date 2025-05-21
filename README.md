package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
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
        try {
            if (fileLocation == null || batchId == null || objectId == null) {
                throw new CustomAppException("Required parameters missing", 400, HttpStatus.BAD_REQUEST);
            }

            // TODO: Replace with Vault secrets
            String accountKey = ""; // getSecretFromVault("account_key", getVaultToken());
            String accountName = "nsndvextr01"; // getSecretFromVault("account_name", ...);
            String containerName = "nsnakscontregecm001"; // getSecretFromVault("container_name", ...);

            String extension = getFileExtension(fileLocation);
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
                long size = sourceBlobClient.getProperties().getBlobSize();
                targetBlobClient.upload(inputStream, size, true);
                logger.info("✅ Uploaded '{}' to '{}'", sourceBlobName, targetBlobClient.getBlobUrl());
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

            try {
                BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                        OffsetDateTime.now().plusHours(24),
                        new BlobSasPermission().setReadPermission(true)
                );
                String sasToken = targetBlobClient.generateSas(sasValues);
                return targetBlobClient.getBlobUrl() + "?" + sasToken;
            } catch (Exception e) {
                logger.error("❌ SAS token generation failed: {}", e.getMessage());
                throw new CustomAppException("Failed to generate SAS URL", 453, HttpStatus.BAD_GATEWAY, e);
            }

        } catch (CustomAppException cae) {
            throw cae; // rethrow
        } catch (Exception e) {
            logger.error("❌ Generic error in uploadFileAndGenerateSasUrl: {}", e.getMessage());
            throw new CustomAppException("Internal blob error", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
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

    private String getFileExtension(String fileLocation) {
        if (fileLocation == null || !fileLocation.contains(".")) {
            return "";
        }
        return fileLocation.substring(fileLocation.lastIndexOf("."));
    }
}
