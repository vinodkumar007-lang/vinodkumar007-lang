if (message.getBatchFiles() == null || message.getBatchFiles().isEmpty()) {
    logger.error("No batch files found in Kafka message.");
    return new ApiResponse("No batch files in message", "error",
            new SummaryPayloadResponse("No batch files in message", "error", new SummaryResponse()).getSummaryResponse());
}

if (file.getBlobUrl() == null || file.getBlobUrl().isEmpty()) {
    logger.warn("Missing blobUrl in batch file: {}", file.getFilename());
    continue;
}
if (summaryFileUrl == null || summaryFileUrl.isEmpty()) {
    logger.error("Summary file upload failed or returned null URL");
    return new ApiResponse("Summary file upload failed", "error",
            new SummaryPayloadResponse("Summary file upload failed", "error", new SummaryResponse()).getSummaryResponse());
}
