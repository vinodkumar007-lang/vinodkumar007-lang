public String buildPrintFileUrl(KafkaMessage message) {
        initSecrets();

        String baseUrl = String.format("https://%s.blob.core.windows.net/%s", accountName, containerName);

        // Format date folder from message timestamp
        String dateFolder = Instant.ofEpochMilli(message.getTimestamp())
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        // Compose print file name or use message info as needed
        String printFileName = message.getBatchId() + "_printfile.pdf";

        // Construct full URL path (adjust folders if needed)
        String printFileUrl = String.format("%s/%s/%s/%s/%s/%s/print/%s",
                baseUrl,
                message.getSourceSystem(),
                dateFolder,
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                message.getJobName(),
                printFileName);

        logger.info("Built print file URL: {}", printFileUrl);
        return printFileUrl;
    }

    /**
     * Uploads the summary JSON file and returns the Blob URL.
     *
     * @param localFilePath Local path of the summary JSON file.
     * @param message       KafkaMessage object containing metadata to construct remote path.
     * @return URL of the uploaded summary JSON file.
     */
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
