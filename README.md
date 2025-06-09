public String uploadSummaryJson(String localFilePathOrUrl, KafkaMessage message) {
        initSecrets();

        if (message == null || message.getBatchId() == null ||
                message.getSourceSystem() == null || message.getUniqueConsumerRef() == null) {
            throw new CustomAppException("Missing Kafka message metadata for uploading summary JSON", 400, HttpStatus.BAD_REQUEST);
        }

        String remoteBlobPath = String.format("%s/%s/%s/summary.json",
                message.getSourceSystem(),
                message.getBatchId(),
                message.getUniqueConsumerRef());

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

     public static String writeSummaryJsonToFile(SummaryPayload payload) {
        try {
            // Create a temp file
            Path tempFile = Files.createTempFile(payload.getBatchID()+"_" +"summary-", ".json");
            // Write JSON content to file
            objectMapper.writeValue(tempFile.toFile(), payload);
            logger.info("✅ Summary JSON successfully written to file: {}", tempFile);
            return tempFile.toString();
        } catch (Exception e) {
            logger.error("❌ Failed to write summary JSON to file: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to write summary JSON to file", e);
        }
    }
