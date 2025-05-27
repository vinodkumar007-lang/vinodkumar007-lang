// 1. Overloaded copyFileFromUrlToBlob with 2 parameters
public String copyFileFromUrlToBlob(String sourceUrl, String targetBlobPath) {
    try {
        initSecrets();

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient targetBlobClient = containerClient.getBlobClient(targetBlobPath);

        try (InputStream inputStream = restTemplate.getForObject(sourceUrl, InputStream.class)) {
            if (inputStream == null) {
                throw new CustomAppException("Unable to read source file from URL: " + sourceUrl, 404, HttpStatus.NOT_FOUND);
            }

            byte[] data = inputStream.readAllBytes();
            targetBlobClient.upload(new java.io.ByteArrayInputStream(data), data.length, true);

            logger.info("✅ Copied '{}' to '{}'", sourceUrl, targetBlobClient.getBlobUrl());
        }

        return targetBlobClient.getBlobUrl();

    } catch (Exception e) {
        logger.error("❌ Error copying file from URL: {}", e.getMessage(), e);
        throw new CustomAppException("Error copying file from URL", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}

// 2. Overloaded uploadFile with 2 parameters
public String uploadFile(String content, String targetBlobPath) {
    try {
        initSecrets();

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.getBlobClient(targetBlobPath);

        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        blobClient.upload(new java.io.ByteArrayInputStream(bytes), bytes.length, true);

        logger.info("✅ Uploaded file to '{}'", blobClient.getBlobUrl());

        return blobClient.getBlobUrl();

    } catch (Exception e) {
        logger.error("❌ Error uploading file: {}", e.getMessage(), e);
        throw new CustomAppException("Error uploading file", 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
