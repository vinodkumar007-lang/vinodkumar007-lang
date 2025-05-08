package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Service
public class AzureBlobStorageService {

    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=your_account_name;AccountKey=your_account_key;EndpointSuffix=core.windows.net";
    private static final String CONTAINER_NAME = "nsnakscontregecm001";

    public void uploadDummyFile(String blobName) {
        try {
            // Create a BlobServiceClient
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(CONNECTION_STRING)
                    .buildClient();

            // Get or create container
            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(CONTAINER_NAME);
            if (!containerClient.exists()) {
                containerClient.create();
            }

            // Prepare dummy file content
            String dummyContent = "This is a dummy file.\nCreated on: " + java.time.LocalDateTime.now();
            byte[] contentBytes = dummyContent.getBytes(StandardCharsets.UTF_8);
            InputStream dataStream = new ByteArrayInputStream(contentBytes);

            // Upload as blob
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.upload(dataStream, contentBytes.length, true); // true = overwrite if exists

            System.out.println("✅ Dummy file uploaded to: " + blobClient.getBlobUrl());

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to upload dummy file", e);
        }
    }

    public static void main(String[] args) {
        AzureBlobStorageService service = new AzureBlobStorageService();
        service.uploadDummyFile("dummy-file.txt"); // You can name this however you like
    }
}
