package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class AzureBlobStorageService {

    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=nsndvextr01;AccountKey=;EndpointSuffix=core.windows.net";
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

        // Convert the byte array into InputStream
        InputStream dataStream = new ByteArrayInputStream(data);

        // Upload using blocking InputStream method
        blobClient.upload(dataStream, data.length, true); // overwrite = true

        System.out.println("✅ File uploaded successfully to Azure Blob Storage: " + blobClient.getBlobUrl());
    }
}
