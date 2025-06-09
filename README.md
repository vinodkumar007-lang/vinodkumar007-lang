public String uploadSummaryJson(String localFilePathOrUrl, KafkaMessage message) {
        initSecrets();

        if (message == null || message.getBatchId() == null ||
                message.getSourceSystem() == null || message.getUniqueConsumerRef() == null) {
            throw new CustomAppException("Missing Kafka message metadata for uploading summary JSON", 400, HttpStatus.BAD_REQUEST);
        }

        // New file name with batchId
        String fileName = "summary_" + message.getBatchId() + ".json";

        // Updated clean remote blob path
        String remoteBlobPath = String.format("%s/%s/%s/%s",
                message.getSourceSystem(),
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                fileName);

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
