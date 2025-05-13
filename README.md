package com.nedbank.kafka.filemanage.service;

import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.config.ProxySetup;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
public class BlobStorageService {

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

    public BlobStorageService(RestTemplate restTemplate, ProxySetup proxySetup) {
        this.restTemplate = restTemplate;
        this.proxySetup = proxySetup;
    }

    public String uploadFileAndGenerateSasUrl(String filePath, String batchId, String objectId) {
        try {
            proxySetup.configureProxy();

            String vaultToken = getVaultToken();
            String accountKey = getSecretFromVault("account_key", vaultToken);
            String accountName = getSecretFromVault("account_name", vaultToken);
            String containerName = getSecretFromVault("container_name", vaultToken);

            String extension = getFileExtension(filePath);
            String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            // Build BlobServiceClient using OkHttp
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .httpClient(new OkHttpAsyncHttpClientBuilder().build())
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            File file = new File(filePath);
            try (InputStream dataStream = new FileInputStream(file)) {
                blobClient.upload(dataStream, file.length(), true);
                System.out.println("✅ File uploaded successfully to Azure Blob Storage: " + blobClient.getBlobUrl());
            }

            // Generate SAS Token
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
            proxySetup.configureProxy();

            String url = VAULT_URL + "/v1/auth/userpass/login/espire_dev";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-vault-namespace", VAULT_NAMESPACE);

            Map<String, String> body = new HashMap<>();
            body.put("password", passwordDev);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

            JSONObject json = new JSONObject(response.getBody());
            return json.getJSONObject("auth").getString("client_token");
        } catch (Exception e) {
            throw new RuntimeException("❌ Failed to obtain Vault token", e);
        }
    }

    private String getSecretFromVault(String key, String token) {
        try {
            proxySetup.configureProxy();

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
            throw new RuntimeException("❌ Failed to retrieve secret from Vault", e);
        }
    }

    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        return lastDotIndex > 0 ? fileLocation.substring(lastDotIndex) : "";
    }
}

<dependencies>
    <!-- Azure SDK -->
    <dependency>
        <groupId>com.azure</groupId>
        <artifactId>azure-storage-blob</artifactId>
        <version>12.10.0</version>
        <exclusions>
            <exclusion>
                <groupId>io.projectreactor.netty</groupId>
                <artifactId>reactor-netty</artifactId>
            </exclusion>
        </exclusions>
    </dependency>

    <dependency>
        <groupId>com.azure</groupId>
        <artifactId>azure-core</artifactId>
        <version>1.14.0</version>
    </dependency>

    <dependency>
        <groupId>com.azure</groupId>
        <artifactId>azure-core-http-okhttp</artifactId>
        <version>1.12.8</version>
    </dependency>

    <!-- Optional, only if used -->
    <dependency>
        <groupId>com.azure</groupId>
        <artifactId>azure-security-keyvault-secrets</artifactId>
        <version>4.2.3</version>
    </dependency>

    <dependency>
        <groupId>com.azure</groupId>
        <artifactId>azure-identity</artifactId>
        <version>1.2.5</version>
    </dependency>

    <!-- Spring Boot and Kafka -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
        <version>3.0.11</version>
    </dependency>

    <!-- Vault integration -->
    <dependency>
        <groupId>org.json</groupId>
        <artifactId>json</artifactId>
        <version>20210307</version>
    </dependency>

    <!-- Jackson -->
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
    </dependency>

    <!-- Lombok -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <scope>provided</scope>
    </dependency>
</dependencies>
