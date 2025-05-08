package com.nedbank.kafka.filemanage.service;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

@Service
public class AzureBlobStorageService {


    public static void main(String[] args) {
            // Initialize BlobServiceClient with connection string

            try {
                BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                        .connectionString("DefaultEndpointsProtocol=http;AccountName=;AccountKey=;EndpointSuffix=core.windows.net")
                        .buildClient();
                var containerClient = blobServiceClient.getBlobContainerClient("nsnakscontregecm001");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }



          /*  // Code to upload a file to Azure Blob Storage
            com.azure.storage.blob.BlobClient blobClient;
            try {
                var containerClient = blobServiceClient.getBlobContainerClient("nsnakscontregecm001");
                //blobClient = containerClient.getBlobClient(blobName);
                // Convert binary data to InputStream
                byte[] binaryData = fileContent.getBytes();
                InputStream inputStream = new ByteArrayInputStream(binaryData);
                blobClient.upload(inputStream, binaryData.length);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }*/
            //return blobClient.getBlobUrl();

    }
}
