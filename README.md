package com.nedbank.kafka.filemanage.config;

import java.net.*;
import java.util.*;

public class ProxySetup {
    public static void configureProxy() {
        String proxyHost = "proxyprod.africa.nedcor.net";//"webproxy.africa.nedcor.net";  // Proxy host
        String proxyPort = "80";//"9001";  // Proxy port
        String proxyUsername = "CC437236";  // Proxy username (if needed)
        String proxyPassword = "34dYaB@jEh56";  // Proxy password (if needed)

        // Set the HTTP/HTTPS proxy system properties
        System.setProperty("http.proxyHost", proxyHost);
        System.setProperty("http.proxyPort", proxyPort);
        System.setProperty("https.proxyHost", proxyHost);
        System.setProperty("https.proxyPort", proxyPort);

        // If proxy requires authentication, you can set up authentication as follows
        if (proxyUsername != null && proxyPassword != null) {
            Authenticator.setDefault(new Authenticator() {
                @Override
                public PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(proxyUsername, proxyPassword.toCharArray());
                }
            });
        }

        // Use system-wide proxy settings (optional)
        System.setProperty("java.net.useSystemProxies", "true");
    }
}
package com.nedbank.kafka.filemanage.service;

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

public class VaultService {

    private final RestTemplate restTemplate;

    public VaultService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public String loginToVault() {
        String url = "https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/auth/userpass/login/espire_dev";

        // Set HTTP headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("x-vault-namespace", "admin/espire");

        // JSON payload
        Map<String, String> body = new HashMap<>();
        body.put("password", "Dev+Cred4#");  // Use your real password

        HttpEntity<Map<String, String>> requestEntity = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    requestEntity,
                    String.class
            );
            return response.getBody();
        } catch (Exception e) {
            e.printStackTrace();
            return "Login failed: " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        // Optional: Configure proxy
        System.setProperty("http.proxyHost", "proxyprod.africa.nedcor.net");
        System.setProperty("http.proxyPort", "80");
        System.setProperty("https.proxyHost", "proxyprod.africa.nedcor.net");
        System.setProperty("https.proxyPort", "80");

        // Create and use service
        RestTemplate restTemplate = new RestTemplate();
        VaultService vaultService = new VaultService(restTemplate);

        String loginResponse = vaultService.loginToVault();
        System.out.println("Vault Login Response:\n" + loginResponse);
    }
}

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
            // Configure proxy settings
            ProxySetup.configureProxy(); // This will configure the system-wide proxy settings
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
            // Create a custom RequestConfig to increase timeouts
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(60000)  // 60 seconds connection timeout
                    .setSocketTimeout(60000)   // 60 seconds socket timeout
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

