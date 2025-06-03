public String downloadFileContent(String blobPathOrUrl) {
    try {
        initSecrets();

        String extractedContainerName = containerName;  // default if container name is configured
        String blobName = blobPathOrUrl;

        // Handle full blob URL
        if (blobPathOrUrl.startsWith("http")) {
            URI uri = new URI(blobPathOrUrl);
            String[] segments = uri.getPath().split("/");

            if (segments.length < 3) {
                throw new CustomAppException("Invalid blob URL format: " + blobPathOrUrl, 400, HttpStatus.BAD_REQUEST);
            }

            // Example: /container/blob => segments[1] = container, segments[2...] = blob path
            extractedContainerName = segments[1];
            blobName = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
        }

        logger.info("🔍 Downloading blob: container='{}', blob='{}'", extractedContainerName, blobName);

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(extractedContainerName);
        BlobClient blobClient = containerClient.getBlobClient(blobName);

        if (!blobClient.exists()) {
            throw new CustomAppException("Blob not found: " + blobName, 404, HttpStatus.NOT_FOUND);
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        blobClient.download(outputStream);

        return outputStream.toString(StandardCharsets.UTF_8.name());

    } catch (Exception e) {
        logger.error("❌ Error downloading blob content for '{}': {}", blobPathOrUrl, e.getMessage(), e);
        throw new CustomAppException("Error downloading blob content", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
