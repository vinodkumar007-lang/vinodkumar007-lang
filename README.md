 public String uploadSummaryJson(String localFilePath, KafkaMessage message) {
        initSecrets();

        // Construct remote blob path for summary.json (adjust folders as per your setup)
        String remoteBlobPath = String.format("%s/%s/%s/summary.json",
                message.getSourceSystem(),
                message.getBatchId(),
                message.getUniqueConsumerRef());

        // Read local file content
        String jsonContent;
        try {
            jsonContent = java.nio.file.Files.readString(java.nio.file.Paths.get(localFilePath));
        } catch (Exception e) {
            logger.error("Error reading summary JSON file at {}: {}", localFilePath, e.getMessage(), e);
            throw new CustomAppException("Error reading summary JSON file", 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }

        // Upload JSON content
        String uploadedUrl = uploadFile(jsonContent, remoteBlobPath);
        logger.info("Uploaded summary JSON to '{}'", uploadedUrl);

        return uploadedUrl;
    }
