package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
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
import java.net.HttpURLConnection;
import java.net.URL;
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

    @Value("${use.proxy:false}")  // Flag to enable/disable proxy configuration
    private boolean useProxy;

    public BlobStorageService(RestTemplate restTemplate, ProxySetup proxySetup) {
        this.restTemplate = restTemplate;
        this.proxySetup = proxySetup;
    }

    public String uploadFileAndGenerateSasUrl(String fileLocation, String batchId, String objectId) {
        try {
            // 🔐 Replace with actual Vault logic in production
            String accountKey = ""; // getSecretFromVault("account_key", getVaultToken());
            String accountName = "nsndvextr01"; // getSecretFromVault("account_name", ...);
            String containerName = "nsnakscontregecm001"; // getSecretFromVault("container_name", ...);

            // 📄 Determine file extension and blob name
            String extension = getFileExtension(fileLocation);
            String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            // 📦 Set up Azure Blob client
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

            // 📥 Get source blob name from URL
            String sourceBlobName = fileLocation.substring(fileLocation.lastIndexOf("/") + 1);
            BlobClient sourceBlobClient = containerClient.getBlobClient(sourceBlobName);

            // ⬇️⬆️ Download source and upload to target
            try (InputStream inputStream = sourceBlobClient.openInputStream()) {
                long sourceSize = sourceBlobClient.getProperties().getBlobSize();
                targetBlobClient.upload(inputStream, sourceSize, true); // true = overwrite
                logger.info("✅ File uploaded successfully from '{}' to '{}'", sourceBlobName, targetBlobClient.getBlobUrl());
            } catch (Exception e) {
                logger.error("❌ Error transferring blob: {}", e.getMessage(), e);
                throw new IOException("❌ Failed to transfer blob from source to target", e);
            }

            // 🔗 Generate SAS token with 24-hour read permission
            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                    OffsetDateTime.now().plusHours(24),
                    new BlobSasPermission().setReadPermission(true)
            );

            String sasToken = targetBlobClient.generateSas(sasValues);
            String sasUrl = targetBlobClient.getBlobUrl() + "?" + sasToken;

            logger.info("🔐 SAS URL (valid for 24 hours): {}", sasUrl);
            return sasUrl;

        } catch (IOException e) {
            logger.error("❌ IO error during blob upload or SAS generation: {}", e.getMessage(), e);
            throw new RuntimeException("❌ Error uploading to Azure Blob or generating SAS URL", e);
        } catch (Exception e) {
            logger.error("❌ Unexpected error during blob operation: {}", e.getMessage(), e);
            throw new RuntimeException("❌ Unexpected error in blob upload or SAS URL generation", e);
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
            logger.error("Error getting Vault token: {}", e.getMessage());
            throw new RuntimeException("❌ Failed to obtain Vault token", e);
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

            JSONObject json = new JSONObject(response.getBody());
            return json.getJSONObject("data").getString(key);
        } catch (Exception e) {
            logger.error("Error retrieving secret from Vault: {}", e.getMessage());
            throw new RuntimeException("❌ Failed to retrieve secret from Vault", e);
        }
    }

    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        return lastDotIndex > 0 ? fileLocation.substring(lastDotIndex) : "";
    }
}
