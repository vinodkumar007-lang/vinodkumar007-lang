package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.BlobClient;
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
