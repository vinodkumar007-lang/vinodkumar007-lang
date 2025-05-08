package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.sas.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

public class AzureBlobStorageService {

    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=;EndpointSuffix=core.windows.net";
    private static final String CONTAINER_NAME = "nsnakscontregecm001";
    private static final String BLOB_NAME = "dummy-file.txt";

    public static void main(String[] args) {
        try {
            uploadDummyFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void uploadDummyFile() {
        // Build the container client
        BlobContainerClient containerClient = new BlobContainerClientBuilder()
                .connectionString(CONNECTION_STRING)
                .containerName(CONTAINER_NAME)
                .buildClient();

        // Create the blob client for a specific blob (file)
        BlobClient blobClient = containerClient.getBlobClient(BLOB_NAME);

        // Create dummy file content
        String dummyFileContent = "This is a dummy file content uploaded to Azure Blob Storage.";
        byte[] data = dummyFileContent.getBytes(StandardCharsets.UTF_8);
        InputStream dataStream = new ByteArrayInputStream(data);

        // Upload the file (overwrite = true)
        blobClient.upload(dataStream, data.length, true);

        System.out.println("✅ File uploaded successfully to Azure Blob Storage: " + blobClient.getBlobUrl());

        // Generate a SAS Token
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                OffsetDateTime.now().plusHours(1), // Expiration time
                new BlobSasPermission().setReadPermission(true) // Read-only access
        );

        // Generate SAS token and append to URL
        String sasToken = blobClient.generateSas(sasValues);
        String sasUrl = blobClient.getBlobUrl() + "?" + sasToken;

        System.out.println("🔐 SAS URL (valid for 1 hour):");
        System.out.println(sasUrl);
    }
}
