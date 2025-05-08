package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.core.util.BinaryData;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class AzureBlobStorageService {

    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=your_account_name;AccountKey=your_account_key;EndpointSuffix=core.windows.net";
    private static final String CONTAINER_NAME = "your-container-name";
    private static final String BLOB_NAME = "dummy-file.txt";

    public static void main(String[] args) {
        try {
            uploadDummyFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void uploadDummyFile() {
        // Create the BlobServiceClient and BlobContainerClient
        BlobContainerClient containerClient = new BlobContainerClientBuilder()
            .connectionString(CONNECTION_STRING)
            .containerName(CONTAINER_NAME)
            .buildClient();

        // Create a BlobClient for the target blob (file)
        BlobClient blobClient = containerClient.getBlobClient(BLOB_NAME);

        // Create a dummy file (in-memory string for this example)
        String dummyFileContent = "This is a dummy file content uploaded to Azure Blob Storage.";

        // Convert the dummy file content to a byte array
        byte[] data = dummyFileContent.getBytes(StandardCharsets.UTF_8);

        // Upload the byte array to the blob storage (blocking API)
        blobClient.upload(BinaryData.fromBytes(data), true);

        System.out.println("File uploaded successfully to Azure Blob Storage: " + BLOB_NAME);
    }
}
