package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class AzureBlobUploadDemo {

    // Your existing method
    public String uploadFileIfDifferent(String sourceBlobName, String batchId, String objectId) {
        try {
            if (sourceBlobName == null || batchId == null || objectId == null) {
                throw new IllegalArgumentException("Required parameters missing");
            }

            String accountKey = ""; // Your account key here or from Vault
            String accountName = "nsndvextr01";
            String containerName = "nsnakscontregecm001";

            String extension = getFileExtension(sourceBlobName);
            String targetBlobName = batchId + "/" + objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);

            BlobClient sourceBlobClient = containerClient.getBlobClient(sourceBlobName);
            if (!sourceBlobClient.exists()) {
                throw new FileNotFoundException("Source blob '" + sourceBlobName + "' does not exist");
            }

            BlobClient targetBlobClient = containerClient.getBlobClient(targetBlobName);

            // If target exists, compare content
            if (targetBlobClient.exists()) {
                byte[] sourceBytes = toByteArray(sourceBlobClient.openInputStream());
                byte[] targetBytes = toByteArray(targetBlobClient.openInputStream());

                if (Arrays.equals(sourceBytes, targetBytes)) {
                    System.out.println("Target blob exists with identical content, skipping upload.");
                    return targetBlobClient.getBlobUrl();
                }
            }

            // Upload (overwrite) target blob
            try (InputStream inputStream = sourceBlobClient.openInputStream()) {
                long size = sourceBlobClient.getProperties().getBlobSize();
                targetBlobClient.upload(inputStream, size, true);
                System.out.println("✅ Uploaded '" + sourceBlobName + "' to '" + targetBlobClient.getBlobUrl() + "'");
            }

            return targetBlobClient.getBlobUrl();

        } catch (BlobStorageException bse) {
            bse.printStackTrace();
            throw new RuntimeException("Azure Blob Storage error: " + bse.getMessage());
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException("I/O error: " + ioe.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unexpected error: " + e.getMessage());
        }
    }

    // Helper method: convert InputStream to byte[]
    private byte[] toByteArray(InputStream input) throws IOException {
        return input.readAllBytes();
    }

    // Helper method: get file extension from blob name
    private String getFileExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? "" : fileName.substring(dotIndex);
    }

    // Main method for testing
    public static void main(String[] args) {
        AzureBlobUploadDemo demo = new AzureBlobUploadDemo();

        String sourceBlobName = "DEBTMAN.csv";  // Change as needed
        String batchId = "batch123";
        String objectId = "object456";

        try {
            String uploadedUrl = demo.uploadFileIfDifferent(sourceBlobName, batchId, objectId);
            System.out.println("Upload completed. Target Blob URL: " + uploadedUrl);
        } catch (Exception e) {
            System.err.println("Upload failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
public String uploadFileAndReturnLocation(String fileLocation, String batchId, String objectId) {
        try {
            if (fileLocation == null || batchId == null || objectId == null) {
                throw new CustomAppException("Required parameters missing", 400, HttpStatus.BAD_REQUEST);
            }

            // TODO: Replace with Vault secrets
            String accountKey = ""; // getSecretFromVault("account_key", getVaultToken());
            String accountName = "nsndvextr01"; // getSecretFromVault("account_name", getVaultToken());
            String containerName = "nsnakscontregecm001"; // getSecretFromVault("container_name", getVaultToken());

            String extension = getFileExtension(fileLocation);

            // Compose blob name with folder structure
            // Format: batchId/objectId_{batchId}.{extension}
            String blobName = batchId + "/" + objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

            String sourceBlobName = fileLocation.substring(fileLocation.lastIndexOf("/") + 1);
            BlobClient sourceBlobClient = containerClient.getBlobClient(sourceBlobName);

            try (InputStream inputStream = sourceBlobClient.openInputStream()) {
                long size = sourceBlobClient.getProperties().getBlobSize();
                targetBlobClient.upload(inputStream, size, true);
                logger.info("✅ Uploaded '{}' to '{}'", sourceBlobName, targetBlobClient.getBlobUrl());
            } catch (BlobStorageException bse) {
                logger.error("❌ Azure Blob Storage error: {}", bse.getMessage());
                throw new CustomAppException("Blob storage operation failed", 453, HttpStatus.BAD_GATEWAY, bse);
            } catch (SocketException se) {
                logger.error("❌ Network error: {}", se.getMessage());
                throw new CustomAppException("Network issue during blob transfer", 420, HttpStatus.GATEWAY_TIMEOUT, se);
            } catch (Exception e) {
                logger.error("❌ Unexpected error during blob transfer: {}", e.getMessage());
                throw new CustomAppException("Unexpected blob error", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
            }

            // Return blob URL without SAS
            return targetBlobClient.getBlobUrl();

        } catch (CustomAppException cae) {
            throw cae; // rethrow
        } catch (Exception e) {
            logger.error("❌ Generic error in uploadFileAndReturnLocation: {}", e.getMessage());
            throw new CustomAppException("Internal blob error", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
