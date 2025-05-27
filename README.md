public String copyFileFromUrlToBlob(String sourceUrl, String targetBlobPath) {
    try {
        initSecrets();

        // Parse sourceUrl to get source account, container, blob path
        URI sourceUri = new URI(sourceUrl);
        String host = sourceUri.getHost(); // e.g. nsndvextr01.blob.core.windows.net
        String[] hostParts = host.split("\\.");
        String sourceAccountName = hostParts[0]; // e.g. nsndvextr01

        String path = sourceUri.getPath(); // e.g. /nsnakscontregecm001/DEBTMAN.csv
        String[] pathParts = path.split("/", 3);
        if (pathParts.length < 3) {
            throw new CustomAppException("Invalid source URL path: " + path, 400, HttpStatus.BAD_REQUEST);
        }
        String sourceContainerName = pathParts[1];
        String sourceBlobPath = pathParts[2];

        // Create source BlobClient
        BlobServiceClient sourceBlobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", sourceAccountName))
                .credential(new StorageSharedKeyCredential(sourceAccountName, accountKey))
                .buildClient();

        BlobContainerClient sourceContainerClient = sourceBlobServiceClient.getBlobContainerClient(sourceContainerName);
        BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(sourceBlobPath);

        // Fallback: check blob size using listBlobs() (avoid BlobProperties)
        long blobSize = -1;
        for (BlobItem item : sourceContainerClient.listBlobs()) {
            if (item.getName().equals(sourceBlobPath)) {
                blobSize = item.getProperties().getContentLength();
                break;
            }
        }

        if (blobSize == -1) {
            throw new CustomAppException("Blob not found: " + sourceBlobPath, 404, HttpStatus.NOT_FOUND);
        }

        logger.info("📄 Source blob '{}' size: {} bytes", sourceUrl, blobSize);

        if (blobSize == 0) {
            logger.warn("⚠️ Source blob '{}' is empty. Skipping copy.", sourceUrl);
            throw new CustomAppException("Source blob is empty: " + sourceUrl, 400, HttpStatus.BAD_REQUEST);
        }

        // Download and prepare input stream
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        sourceBlobClient.download(outputStream);
        byte[] sourceBlobBytes = outputStream.toByteArray();
        InputStream inputStream = new ByteArrayInputStream(sourceBlobBytes);

        // Target blob client
        BlobServiceClient targetBlobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobContainerClient targetContainerClient = targetBlobServiceClient.getBlobContainerClient(containerName);
        BlobClient targetBlobClient = targetContainerClient.getBlobClient(targetBlobPath);

        targetBlobClient.upload(inputStream, sourceBlobBytes.length, true);

        logger.info("✅ Copied '{}' to '{}'", sourceUrl, targetBlobClient.getBlobUrl());

        return targetBlobClient.getBlobUrl();

    } catch (Exception e) {
        logger.error("❌ Error copying file from URL: {}", e.getMessage(), e);
        throw new CustomAppException("Error copying file from URL", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
