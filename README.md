public String uploadSummaryJson(String localFilePathOrUrl, KafkaMessage message) {
    initSecrets();

    String remoteBlobPath = String.format("%s/%s/%s/summary.json",
            message.getSourceSystem(),
            message.getBatchId(),
            message.getUniqueConsumerRef());

    String jsonContent;

    try {
        if (localFilePathOrUrl.startsWith("http://") || localFilePathOrUrl.startsWith("https://")) {
            // It's a URL, download content via HTTP
            jsonContent = downloadContentFromUrl(localFilePathOrUrl);
        } else {
            // It's a local file path, read from file system
            jsonContent = java.nio.file.Files.readString(java.nio.file.Paths.get(localFilePathOrUrl));
        }
    } catch (Exception e) {
        logger.error("Error reading summary JSON content at {}: {}", localFilePathOrUrl, e.getMessage(), e);
        throw new CustomAppException("Error reading summary JSON content", 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }

    // Upload JSON content to Azure Blob Storage
    String uploadedUrl = uploadFile(jsonContent, remoteBlobPath);
    logger.info("Uploaded summary JSON to '{}'", uploadedUrl);

    return uploadedUrl;
}

// Helper method to download content from URL
private String downloadContentFromUrl(String urlString) throws IOException {
    try (InputStream in = new URL(urlString).openStream()) {
        return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
}
