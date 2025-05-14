public String uploadFileAndGenerateSasUrl(String fileLocation, String batchId, String objectId) {
    try {
        // Configure proxy setup dynamically based on the useProxy flag
        proxySetup.configureProxy(useProxy);

        // Get secrets from Vault (temporarily using hardcoded/test values for now)
        // String vaultToken = getVaultToken();
        String accountKey = ""; // getSecretFromVault("account_key", vaultToken);
        String accountName = "nsndvextr01"; // getSecretFromVault("account_name", vaultToken);
        String containerName = "nsnakscontregecm001"; // getSecretFromVault("container_name", vaultToken);

        // Extract the file extension from the URL
        String extension = getFileExtension(fileLocation);
        String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

        // Build BlobServiceClient
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.getBlobClient(blobName);

        // Log if blob already exists
        if (blobClient.exists()) {
            logger.warn("⚠️ Blob already exists. It will be overwritten: {}", blobName);
        }

        // ⬇️ Download the file from the URL and upload to Azure Blob
        try (InputStream inputStream = new URL(fileLocation).openStream()) {
            blobClient.upload(inputStream, inputStream.available(), true); // ✅ Overwrite if exists
            logger.info("✅ File uploaded successfully to Azure Blob Storage: {}", blobClient.getBlobUrl());
        } catch (IOException e) {
            logger.error("❌ Error downloading the file from the provided URL: {}", fileLocation);
            throw new IOException("❌ Error downloading the file from the provided URL", e);
        }

        // 🔐 Generate SAS URL with read permission valid for 24 hours
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                OffsetDateTime.now().plusHours(24),
                new BlobSasPermission().setReadPermission(true)
        );

        String sasToken = blobClient.generateSas(sasValues);
        String sasUrl = blobClient.getBlobUrl() + "?" + sasToken;

        logger.info("🔐 SAS URL (valid for 24 hours): {}", sasUrl);
        return sasUrl;

    } catch (IOException e) {
        logger.error("❌ Error during file upload or SAS URL generation: {}", e.getMessage(), e);
        throw new RuntimeException("❌ Error uploading to Azure Blob or generating SAS URL", e);
    } catch (Exception e) {
        logger.error("❌ Unexpected error in Blob operation: {}", e.getMessage(), e);
        throw new RuntimeException("❌ Unexpected error in Blob upload or SAS URL generation", e);
    }
}
