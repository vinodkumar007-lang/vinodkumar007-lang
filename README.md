public String copyFileFromUrlToBlob(String sourceUrl, String targetBlobPath) {
    try {
        initSecrets();

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient targetBlobClient = containerClient.getBlobClient(targetBlobPath);

        InputStream inputStream = restTemplate.execute(sourceUrl, HttpMethod.GET, null, clientHttpResponse -> {
            if (clientHttpResponse.getStatusCode() != HttpStatus.OK) {
                throw new CustomAppException("Failed to fetch source file, status: " + clientHttpResponse.getStatusCode(), 404, HttpStatus.NOT_FOUND);
            }
            return clientHttpResponse.getBody();
        });

        if (inputStream == null) {
            throw new CustomAppException("Unable to read source file from URL: " + sourceUrl, 404, HttpStatus.NOT_FOUND);
        }

        // Upload the stream directly without buffering entire file in memory
        targetBlobClient.upload(inputStream, -1, true);  // -1 means unknown length, stream upload

        logger.info("✅ Copied '{}' to '{}'", sourceUrl, targetBlobClient.getBlobUrl());

        return targetBlobClient.getBlobUrl();

    } catch (Exception e) {
        logger.error("❌ Error copying file from URL: {}", e.getMessage(), e);
        throw new CustomAppException("Error copying file from URL", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
