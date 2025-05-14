package com.nedbank.kafka.filemanage.config;

import org.springframework.stereotype.Component;

import java.net.*;
import java.util.Optional;

@Component
public class ProxySetup {

    private final ProxyProperties proxyProperties;
    private static boolean configured = false;

    public ProxySetup(ProxyProperties proxyProperties) {
        this.proxyProperties = proxyProperties;
    }

    public void configureProxy(boolean useProxy) {
        if (useProxy && !configured) { // Only configure proxy if it's necessary
            String host = proxyProperties.getHost();
            String port = proxyProperties.getPort();
            String username = proxyProperties.getUsername();
            String password = proxyProperties.getPassword();

            if (host == null || port == null || host.isEmpty() || port.isEmpty()) {
                System.err.println("⚠️ Proxy settings are missing. Proxy not configured.");
                return;
            }

            System.setProperty("http.proxyHost", host);
            System.setProperty("http.proxyPort", port);
            System.setProperty("https.proxyHost", host);
            System.setProperty("https.proxyPort", port);
            System.setProperty("java.net.useSystemProxies", "true");

            if (username != null && password != null && !username.isEmpty() && !password.isEmpty()) {
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray());
                    }
                });
            }

            System.out.println("🔧 Proxy configured from ProxyProperties: " + host + ":" + port);
            configured = true;
        } else if (!useProxy) {
            // Clear proxy settings if not using a proxy
            System.clearProperty("http.proxyHost");
            System.clearProperty("http.proxyPort");
            System.clearProperty("https.proxyHost");
            System.clearProperty("https.proxyPort");
            System.clearProperty("java.net.useSystemProxies");
            Authenticator.setDefault(null); // Reset authenticator
            System.out.println("⚙️ Proxy configuration skipped.");
        }
    }
}

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
            // Configure proxy setup dynamically based on the useProxy flag
            proxySetup.configureProxy(useProxy);

            // Get secrets from Vault
            String vaultToken = getVaultToken();
            String accountKey = getSecretFromVault("account_key", vaultToken);
            String accountName = getSecretFromVault("account_name", vaultToken);
            String containerName = getSecretFromVault("container_name", vaultToken);

            // Extract the file extension from the URL (if any)
            String extension = getFileExtension(fileLocation);
            String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            // Build BlobServiceClient using the account credentials
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            // Get the BlobContainerClient to interact with the container
            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);

            // Get BlobClient for the given blob name (we'll use this to check if it exists and upload)
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            // Check if the blob already exists (based on the blob name)
            if (blobClient.exists()) {
                logger.info("The file already exists in Azure Blob Storage. Updating the content...");
            }

            // ⬇️ Download the file from the provided file location (URL from Kafka)
            try (InputStream inputStream = new URL(fileLocation).openStream()) {
                // Upload or overwrite the file to Azure Blob Storage
                blobClient.upload(inputStream, inputStream.available(), true); // Overwrite if exists
                logger.info("✅ File uploaded successfully to Azure Blob Storage: {}", blobClient.getBlobUrl());
            } catch (IOException e) {
                throw new IOException("❌ Error downloading the file from the provided URL", e);
            }

            // 🔐 Generate SAS Token with 24-hour read access
            BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                    OffsetDateTime.now().plusHours(24),
                    new BlobSasPermission().setReadPermission(true)
            );

            // Generate the SAS token for the blob
            String sasToken = blobClient.generateSas(sasValues);
            String sasUrl = blobClient.getBlobUrl() + "?" + sasToken;

            logger.info("🔐 SAS URL (valid for 24 hours): {}", sasUrl);
            return sasUrl;

        } catch (IOException e) {
            logger.error("Error during file upload or SAS URL generation: {}", e.getMessage());
            throw new RuntimeException("❌ Error uploading to Azure Blob or generating SAS URL", e);
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
