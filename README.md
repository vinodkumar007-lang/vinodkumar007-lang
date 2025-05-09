package com.nedbank.kafka.filemanage.service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    private final SecretClient secretClient;

    public BlobStorageService(@Value("${azure.keyvault.uri}") String keyVaultUri) {
        this.secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUri)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();
    }

    public String uploadFile(String filePath, String batchId) throws Exception {
        logger.info("Fetching secrets from Azure Key Vault...");

        String accountName = getSecretValue("accountName");
        String accountKey = getSecretValue("accountKey");
        String containerName = getSecretValue("containerName");

        logger.info("Fetched accountName={}, containerName={}", accountName, containerName);

        String connectionString = String.format(
                "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
                accountName, accountKey
        );

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        if (!containerClient.exists()) {
            containerClient.create();
        }

        File file = new File(filePath);
        if (!file.exists()) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }

        String blobName = batchId + "/" + file.getName();
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.uploadFromFile(file.getAbsolutePath(), true);

        String blobUrl = blobClient.getBlobUrl();
        logger.info("File uploaded to blob storage at URL: {}", blobUrl);

        return blobUrl;
    }

    private String getSecretValue(String secretName) {
        KeyVaultSecret secret = secretClient.getSecret(secretName);
        return secret.getValue();
    }
}
