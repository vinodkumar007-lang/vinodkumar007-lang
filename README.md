public static String writeSummaryJsonToFile(SummaryPayload payload) {
    if (payload == null) {
        logger.error("❌ SummaryPayload is null. Cannot write summary.json.");
        throw new IllegalArgumentException("SummaryPayload cannot be null");
    }

    try {
        // 🔍 Golden Thread: validate mandatory fields
        if (payload.getHeader() == null) {
            logger.warn("⚠️ SummaryPayload.header is null.");
        }
        if (payload.getMetadata() == null) {
            logger.warn("⚠️ SummaryPayload.metadata is null.");
        }
        if (payload.getProcessedFiles() == null || payload.getProcessedFiles().isEmpty()) {
            logger.warn("⚠️ No processedFiles found in payload.");
        }

        String batchId = Optional.ofNullable(payload.getBatchID()).orElse("unknown");
        String fileName = "summary_" + batchId + ".json";

        // Create temp dir and resolve full path
        Path tempDir = Files.createTempDirectory("summaryFiles");
        Path summaryFilePath = tempDir.resolve(fileName);

        File summaryFile = summaryFilePath.toFile();
        if (summaryFile.exists()) {
            Files.delete(summaryFilePath);
            logger.warn("Existing summary file deleted: {}", summaryFilePath);
        }

        // ✅ Write JSON
        objectMapper.writeValue(summaryFile, payload);
        logger.info("✅ Summary JSON written successfully at: {}", summaryFilePath);

        return summaryFilePath.toAbsolutePath().toString();

    } catch (Exception e) {
        logger.error("❌ Failed to write summary.json", e);
        throw new RuntimeException("Failed to write summary JSON", e);
    }
}
