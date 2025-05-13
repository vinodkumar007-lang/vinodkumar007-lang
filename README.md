package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.nedbank.kafka.filemanage.config.ProxySetup;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.OffsetDateTime;

@Service
public class BlobStorageService {

    private final VaultClientService vaultClient;
    private final ProxySetup proxySetup;

    public BlobStorageService(VaultClientService vaultClient, ProxySetup proxySetup) {
        this.vaultClient = vaultClient;
        this.proxySetup = proxySetup;
    }

    public String uploadFileAndGenerateSasUrl(String filePath, String batchId, String objectId) {
        try {
            // Configure proxy if needed
            proxySetup.configureProxy();
            System.out.println("Proxy Host: " + System.getProperty("http.proxyHost"));
            System.out.println("Proxy Port: " + System.getProperty("http.proxyPort"));

            // Authenticate with Vault and retrieve secrets
            String vaultToken = vaultClient.getVaultToken();
            String accountKey = vaultClient.getSecret("Store_Dev/10099", "account_key", vaultToken);
            String accountName = vaultClient.getSecret("Store_Dev/10099", "account_name", vaultToken);
            String containerName = vaultClient.getSecret("Store_Dev/10099", "container_name", vaultToken);

            // Construct blob name
            String extension = getFileExtension(filePath);
            String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            // Build Azure connection string
            String connectionString = String.format(
                "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
                accountName, accountKey
            );

            // Upload the file to Azure Blob Storage
            BlobContainerClient containerClient = new BlobContainerClientBuilder()
                    .connectionString(connectionString)
                    .containerName(containerName)
                    .buildClient();

            BlobClient blobClient = containerClient.getBlobClient(blobName);

            File file = new File(filePath);
            try (InputStream dataStream = new FileInputStream(file)) {
                blobClient.upload(dataStream, file.length(), true);
                System.out.println("✅ File uploaded successfully to Azure Blob Storage: " + blobClient.getBlobUrl());
            }

            // Generate SAS token
            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                    OffsetDateTime.now().plusHours(24),
                    new BlobSasPermission().setReadPermission(true)
            );

            String sasToken = blobClient.generateSas(sasValues);
            String sasUrl = blobClient.getBlobUrl() + "?" + sasToken;

            System.out.println("🔐 SAS URL (valid for 24 hours):");
            System.out.println(sasUrl);

            return sasUrl;
        } catch (Exception e) {
            throw new RuntimeException("❌ Error uploading to Azure Blob or generating SAS URL", e);
        }
    }

    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        if (lastDotIndex > 0) {
            return fileLocation.substring(lastDotIndex);
        } else {
            return "";
        }
    }
}

package com.nedbank.kafka.filemanage.service;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
public class VaultClientService {

    private final RestTemplate restTemplate;

    @Value("${vault.hashicorp.url}")
    private String VAULT_URL;

    @Value("${vault.hashicorp.namespace}")
    private String VAULT_NAMESPACE;

    @Value("${vault.hashicorp.passwordDev}")
    private String passwordDev;

    @Value("${vault.hashicorp.passwordNbhDev}")
    private String passwordNbhDev;

    public VaultClientService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public String getVaultToken() {
        try {
            String url = VAULT_URL + "/v1/auth/userpass/login/espire_dev";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-vault-namespace", VAULT_NAMESPACE);

            Map<String, String> body = new HashMap<>();
            body.put("password", passwordDev);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                JSONObject json = new JSONObject(response.getBody());
                return json.getJSONObject("auth").getString("client_token");
            } else {
                throw new RuntimeException("Vault login failed with status: " + response.getStatusCode());
            }

        } catch (Exception e) {
            throw new RuntimeException("❌ Failed to obtain Vault token", e);
        }
    }

    public String getSecret(String path, String key, String token) {
        try {
            String url = VAULT_URL + "/v1/" + path;

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-vault-namespace", VAULT_NAMESPACE);
            headers.set("x-vault-token", token);

            Map<String, String> body = new HashMap<>();
            body.put("password", passwordNbhDev);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                JSONObject json = new JSONObject(response.getBody());
                return json.getJSONObject("data").getString(key);
            } else {
                throw new RuntimeException("Vault secret fetch failed with status: " + response.getStatusCode());
            }

        } catch (Exception e) {
            throw new RuntimeException("❌ Failed to retrieve secret from Vault", e);
        }
    }
}
