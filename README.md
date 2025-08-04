private String findAndUploadMobstatTriggerFile(Path jobDir, KafkaMessage message) {
    try (Stream<Path> stream = Files.list(jobDir)) {
        Optional<Path> trigger = stream.filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".trigger"))
                .findFirst();

        if (trigger.isPresent()) {
            Path triggerFile = trigger.get();
            String blobUrl = blobStorageService.uploadFile(triggerFile.toFile(),
                    message.getSourceSystem() + "/" + message.getBatchId() + "/" + triggerFile.getFileName());

            logger.info("📤 Uploaded MOBSTAT trigger file: {} -> {}", triggerFile, blobUrl);
            return decodeUrl(blobUrl);
        } else {
            logger.warn("⚠️ No .trigger file found in MOBSTAT job directory: {}", jobDir);
            return null;
        }

    } catch (IOException e) {
        logger.error("⚠️ Failed to scan for .trigger file in jobDir: {}", jobDir, e);
        return null; // ✅ changed to avoid stopping program
    }
}
