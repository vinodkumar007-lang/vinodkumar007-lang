package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.config.ProxySetup;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.*;
import org.apache.http.client.config.*;
import org.apache.http.entity.*;
import org.apache.http.util.*;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

@Service
public class BlobStorageService {

    @Value("${vault.hashicorp.url}")
    private String VAULT_URL;

    @Value("${vault.hashicorp.namespace}")
    private String VAULT_NAMESPACE;

    @Value("${vault.hashicorp.passwordDev}")
    private String passwordDev;

    @Value("${vault.hashicorp.passwordNbhDev}")
    private String passwordNbhDev;

    public String uploadFileAndGenerateSasUrl(String filePath, String batchId, String objectId) {
        try {
            // 🔧 Configure proxy settings
            ProxySetup.configureProxy();
            System.out.println("Proxy Host: " + System.getProperty("http.proxyHost"));
            System.out.println("Proxy Port: " + System.getProperty("http.proxyPort"));

            String vaultToken = getVaultToken();

            String accountKey = getSecretFromVault("account_key", vaultToken);
            String accountName = getSecretFromVault("account_name", vaultToken);
            String containerName = getSecretFromVault("container_name", vaultToken);

            String extension = getFileExtension(filePath);
            String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            String connectionString = String.format(
                    "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
                    accountName, accountKey
            );

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

    private String getVaultToken() {
        try {
            // 🔧 Configure proxy for Vault login
            ProxySetup.configureProxy();

            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(60000)
                    .setSocketTimeout(60000)
                    .build();

            CloseableHttpClient client = HttpClients.custom()
                    .setDefaultRequestConfig(requestConfig)
                    .build();

            HttpPost post = new HttpPost(VAULT_URL + "/v1/auth/userpass/login/espire_dev");
            post.setHeader("x-vault-namespace", VAULT_NAMESPACE);
            post.setHeader("Content-Type", "application/json");

            StringEntity entity = new StringEntity("{ \"password\": \"" + passwordDev + "\" }");
            post.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(post)) {
                String jsonResponse = EntityUtils.toString(response.getEntity());
                JSONObject jsonObject = new JSONObject(jsonResponse);
                return jsonObject.getJSONObject("auth").getString("client_token");
            }
        } catch (Exception e) {
            throw new RuntimeException("❌ Failed to obtain Vault token", e);
        }
    }

    private String getSecretFromVault(String key, String token) {
        try {
            // 🔧 Configure proxy for Vault secret fetch
            ProxySetup.configureProxy();

            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpPost post = new HttpPost(VAULT_URL + "/v1/Store_Dev/10099");
                post.setHeader("x-vault-namespace", VAULT_NAMESPACE);
                post.setHeader("x-vault-token", token);
                post.setHeader("Content-Type", "application/json");

                StringEntity entity = new StringEntity("{ \"password\": \"" + passwordNbhDev + "\" }");
                post.setEntity(entity);

                try (CloseableHttpResponse response = client.execute(post)) {
                    String jsonResponse = EntityUtils.toString(response.getEntity());
                    JSONObject jsonObject = new JSONObject(jsonResponse);
                    return jsonObject.getJSONObject("data").getString(key);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("❌ Failed to retrieve secret from Vault", e);
        }
    }

    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        if (lastDotIndex > 0) {
            return fileLocation.substring(lastDotIndex);
        } else {
            return ""; // Default to empty string if no extension is found
        }
    }
}
