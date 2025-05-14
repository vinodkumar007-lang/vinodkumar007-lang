public String uploadFileAndGenerateSasUrl(String fileLocation, String batchId, String objectId) {
    try {
        // 🔐 Replace with actual Vault logic in production
        String accountKey = ""; // getSecretFromVault("account_key", getVaultToken());
        String accountName = "nsndvextr01"; // getSecretFromVault("account_name", ...);
        String containerName = "nsnakscontregecm001"; // getSecretFromVault("container_name", ...);

        // 📄 Determine file extension and blob name
        String extension = getFileExtension(fileLocation);
        String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

        // 📦 Set up Azure Blob client
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

        // 📥 Get source blob name from URL
        String sourceBlobName = fileLocation.substring(fileLocation.lastIndexOf("/") + 1);
        BlobClient sourceBlobClient = containerClient.getBlobClient(sourceBlobName);

        // ⬇️⬆️ Download source and upload to target
        try (InputStream inputStream = sourceBlobClient.openInputStream()) {
            long sourceSize = sourceBlobClient.getProperties().getBlobSize();
            targetBlobClient.upload(inputStream, sourceSize, true); // true = overwrite
            logger.info("✅ File uploaded successfully from '{}' to '{}'", sourceBlobName, targetBlobClient.getBlobUrl());
        } catch (Exception e) {
            logger.error("❌ Error transferring blob: {}", e.getMessage(), e);
            throw new IOException("❌ Failed to transfer blob from source to target", e);
        }

        // 🔗 Generate SAS token with 24-hour read permission
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                OffsetDateTime.now().plusHours(24),
                new BlobSasPermission().setReadPermission(true)
        );

        String sasToken = targetBlobClient.generateSas(sasValues);
        String sasUrl = targetBlobClient.getBlobUrl() + "?" + sasToken;

        logger.info("🔐 SAS URL (valid for 24 hours): {}", sasUrl);
        return sasUrl;

    } catch (IOException e) {
        logger.error("❌ IO error during blob upload or SAS generation: {}", e.getMessage(), e);
        throw new RuntimeException("❌ Error uploading to Azure Blob or generating SAS URL", e);
    } catch (Exception e) {
        logger.error("❌ Unexpected error during blob operation: {}", e.getMessage(), e);
        throw new RuntimeException("❌ Unexpected error in blob upload or SAS URL generation", e);
    }
}
