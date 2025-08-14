private String findAndUploadMobstatTriggerFile(Path jobDir, KafkaMessage message) {
    // ✅ First check if the directory exists and is actually a folder
    if (jobDir == null || !Files.exists(jobDir) || !Files.isDirectory(jobDir)) {
        logger.warn("⚠️ MOBSTAT job directory does not exist or is not a directory: {}", jobDir);
        return null; // Skip gracefully
    }

    try (Stream<Path> stream = Files.list(jobDir)) {
        Optional<Path> trigger = stream
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().toLowerCase().endsWith(TRIGGER_FILE_EXTENSION))
                .findFirst();

        if (trigger.isPresent()) {
            Path triggerFile = trigger.get();
            try {
                String blobUrl = blobStorageService.uploadFile(
                        triggerFile.toFile(),
                        String.format(MOBSTAT_TRIGGER_UPLOAD_PATH_FORMAT,
                                message.getSourceSystem(), message.getBatchId(),
                                message.getUniqueConsumerRef(), triggerFile.getFileName())
                );

                logger.info("📤 Uploaded MOBSTAT trigger file: {} -> {}", triggerFile, blobUrl);
                return decodeUrl(blobUrl);
            } catch (Exception uploadEx) {
                logger.error("⚠️ Failed to upload MOBSTAT trigger file: {}", triggerFile, uploadEx);
                // Continue anyway
                return null;
            }
        } else {
            logger.warn("⚠️ No .trigger file found in MOBSTAT job directory: {}", jobDir);
            return null;
        }

    } catch (IOException e) {
        logger.error("⚠️ Error scanning for .trigger file in jobDir: {}", jobDir, e);
        return null; // Continue gracefully
    }
}
