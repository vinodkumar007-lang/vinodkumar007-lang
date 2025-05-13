public String uploadFileAndGenerateSasUrl(String filePath, String batchId, String objectId) {
    try {
        proxySetup.configureProxy();

        String vaultToken = getVaultToken();
        String accountKey = getSecretFromVault("account_key", vaultToken);
        String accountName = getSecretFromVault("account_name", vaultToken);
        String containerName = getSecretFromVault("container_name", vaultToken);

        String extension = getFileExtension(filePath);
        String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

        // Create BlobServiceClient using OkHttp
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .httpClient(new OkHttpAsyncHttpClientBuilder().build())
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.getBlobClient(blobName);

        // Download the file from the URL (filePath)
        System.out.println("🌐 Downloading file from: " + filePath);
        URL url = new URL(filePath);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        try (InputStream inputStream = connection.getInputStream()) {
            // Upload to Azure Blob Storage (overwrite = true)
            blobClient.upload(inputStream, connection.getContentLengthLong(), true);
            System.out.println("✅ File uploaded (or updated) successfully to: " + blobClient.getBlobUrl());
        }

        // Generate SAS token for the uploaded blob
        BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                OffsetDateTime.now().plusHours(24),
                new BlobSasPermission().setReadPermission(true)
        );
        String sasToken = blobClient.generateSas(sasValues);
        String sasUrl = blobClient.getBlobUrl() + "?" + sasToken;

        System.out.println("🔐 SAS URL (valid for 24 hours): " + sasUrl);
        return sasUrl;

    } catch (Exception e) {
        throw new RuntimeException("❌ Error uploading to Azure Blob or generating SAS URL", e);
    }
}
