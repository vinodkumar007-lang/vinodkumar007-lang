public String downloadFileContent(String blobPath) {
    try {
        initSecrets();

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.getBlobClient(blobPath);

        if (!blobClient.exists()) {
            throw new CustomAppException("Blob not found: " + blobPath, 404, HttpStatus.NOT_FOUND);
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blobClient.download(outputStream);

        return outputStream.toString(StandardCharsets.UTF_8.name());

    } catch (Exception e) {
        logger.error("❌ Error downloading blob content for '{}': {}", blobPath, e.getMessage(), e);
        throw new CustomAppException("Error downloading blob content", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
