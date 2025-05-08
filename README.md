package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.core.util.BinaryData;
import java.nio.charset.StandardCharsets;

public class AzureBlobStorageService {

    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=your_account_name;AccountKey=EndpointSuffix=core.windows.net";
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
        // The length of the data (in bytes) is passed as the second parameter
        blobClient.upload(BinaryData.fromBytes(data).toStream(), data.length, true);  // 'data.length' specifies the content length

        System.out.println("File uploaded successfully to Azure Blob Storage: " + BLOB_NAME);
    }
}

Exception in thread "main" java.lang.NoSuchMethodError: 'reactor.core.publisher.Mono reactor.core.publisher.Mono.subscriberContext(reactor.util.context.Context)'
	at com.azure.storage.blob.BlobClient.uploadWithResponse(BlobClient.java:229)
	at com.azure.storage.blob.BlobClient.uploadWithResponse(BlobClient.java:195)
	at com.azure.storage.blob.BlobClient.upload(BlobClient.java:169)
	at com.nedbank.kafka.filemanage.service.AzureBlobStorageService.uploadDummyFile(AzureBlobStorageService.java:41)
	at com.nedbank.kafka.filemanage.service.AzureBlobStorageService.main(AzureBlobStorageService.java:17)
