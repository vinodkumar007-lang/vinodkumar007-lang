public String uploadSummaryJson(String localFilePathOrUrl, KafkaMessage message, String summaryFileName) {
    initSecrets();

    if (message == null || message.getBatchId() == null ||
            message.getSourceSystem() == null || message.getUniqueConsumerRef() == null) {
        throw new CustomAppException("Missing Kafka message metadata for uploading summary JSON", 400, HttpStatus.BAD_REQUEST);
    }

    // Use the provided summaryFileName directly (e.g. "summary_<batchId>.json")
    String remoteBlobPath = String.format("%s/%s/%s/%s",
            message.getSourceSystem(),
            message.getBatchId(),
            message.getUniqueConsumerRef(),
            summaryFileName);

    String jsonContent;
    try {
        if (localFilePathOrUrl.startsWith("http://") || localFilePathOrUrl.startsWith("https://")) {
            jsonContent = downloadContentFromUrl(localFilePathOrUrl);
        } else {
            jsonContent = Files.readString(Paths.get(localFilePathOrUrl));
        }
    } catch (Exception e) {
        logger.error("❌ Error reading summary JSON content: {}", e.getMessage(), e);
        throw new CustomAppException("Error reading summary JSON content", 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }

    return uploadFile(jsonContent, remoteBlobPath);
}
