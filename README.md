public String uploadFileAndGenerateSasUrl(String fileLocation, String batchId, String objectId) {
    try {
        // Setup Proxy if needed
        proxySetup.configureProxy();

        // Get secrets from Vault
        String vaultToken = getVaultToken();
        String accountKey = getSecretFromVault("account_key", vaultToken);
        String accountName = getSecretFromVault("account_name", vaultToken);
        String containerName = getSecretFromVault("container_name", vaultToken);

        // Extract the file extension from the URL (if any)
        String extension = getFileExtension(fileLocation);
        String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

        // Build BlobServiceClient using the account credentials
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .httpClient(new OkHttpAsyncHttpClientBuilder().build())
                .buildClient();

        // Get the BlobContainerClient to interact with the container
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);

        // Get BlobClient for the given blob name (we'll use this to check if it exists and upload)
        BlobClient blobClient = containerClient.getBlobClient(blobName);

        // Check if the blob already exists (based on the blob name)
        if (blobClient.exists()) {
            System.out.println("The file already exists in Azure Blob Storage. Updating the content...");
        }

        // ⬇️ Download the file from the provided file location (URL from Kafka)
        try (InputStream inputStream = new URL(fileLocation).openStream()) {
            // Upload or overwrite the file to Azure Blob Storage
            blobClient.upload(inputStream, inputStream.available(), true); // Overwrite if exists
            System.out.println("✅ File uploaded successfully to Azure Blob Storage: " + blobClient.getBlobUrl());
        } catch (IOException e) {
            throw new RuntimeException("❌ Error downloading the file from the provided URL", e);
        }

        // 🔐 Generate SAS Token with 24-hour read access
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                OffsetDateTime.now().plusHours(24),
                new BlobSasPermission().setReadPermission(true)
        );

        // Generate the SAS token for the blob
        String sasToken = blobClient.generateSas(sasValues);
        String sasUrl = blobClient.getBlobUrl() + "?" + sasToken;

        System.out.println("🔐 SAS URL (valid for 24 hours): " + sasUrl);
        return sasUrl;

    } catch (Exception e) {
        // Log the exception for debugging
        e.printStackTrace();
        throw new RuntimeException("❌ Error uploading to Azure Blob or generating SAS URL", e);
    }
}
