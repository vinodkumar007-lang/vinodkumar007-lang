private ApiResponse processSingleMessage(KafkaMessage message) throws UnsupportedEncodingException {
    if (message == null || message.getBatchFiles() == null || message.getBatchFiles().isEmpty()) {
        return new ApiResponse("Empty or invalid message", "error", null);
    }

    List<BatchFile> validFiles = message.getBatchFiles().stream()
            .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
            .toList();

    if (validFiles.isEmpty()) {
        return new ApiResponse("No DATA files found", "error", null);
    }

    String batchId = message.getBatchId();
    String guiRef = message.getUniqueConsumerRef();

    for (BatchFile file : validFiles) {
        String blobUrl = file.getBlobUrl();
        try {
            String fileName = extractFileName(blobUrl);
            Path localMountPath = Path.of(MOUNT_PATH_BASE, batchId, guiRef);
            Files.createDirectories(localMountPath);

            Path targetFilePath = localMountPath.resolve(fileName);
            String content = blobStorageService.downloadFileContent(blobUrl);
            Files.write(targetFilePath, content.getBytes(StandardCharsets.UTF_8));

            logger.info("📁 Saved DATA file to mount: {}", targetFilePath);

            // Replace blobUrl with mount path
            file.setBlobUrl(targetFilePath.toString());

        } catch (Exception ex) {
            logger.error("❌ Failed to mount blob file [batchId={}, guiRef={}, url={}]", batchId, guiRef, blobUrl, ex);
            return new ApiResponse("Failed to mount file: " + blobUrl, "error", null);
        }
    }

    try {
        String updatedJson = objectMapper.writeValueAsString(message);

        // 🔐 Prepare headers with Authorization
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer eyJraWQiOiJjZjkwMjJmMjUxNjM2MjQzNjI5YmE1ZmNmMjMwZDI4YzFlOTJkNDNiIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ...");
        headers.set("Content-Type", "application/json");

        HttpEntity<String> request = new HttpEntity<>(updatedJson, headers);

        logger.info("📤 Sending metadata.json to OpenText API at: {}", OPENTEXT_API_URL);

        restTemplate.postForEntity(OPENTEXT_API_URL, request, String.class);

        return new ApiResponse("Sent metadata to OT", "success", null);
    } catch (Exception e) {
        logger.error("❌ Failed to send metadata.json to OT [batchId={}, guiRef={}]", message.getBatchId(), message.getUniqueConsumerRef(), e);
        return new ApiResponse("Failed to call OT API", "error", null);
    }
}
