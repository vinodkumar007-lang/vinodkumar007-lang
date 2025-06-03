public static String writeSummaryJsonToFile(SummaryPayload payload) {
    try {
        // Create a temp file
        Path tempFile = Files.createTempFile("summary-", ".json");
        // Write JSON content to file
        objectMapper.writeValue(tempFile.toFile(), payload);
        logger.info("✅ Summary JSON successfully written to file: {}", tempFile);
        return tempFile.toString();
    } catch (Exception e) {
        logger.error("❌ Failed to write summary JSON to file: {}", e.getMessage(), e);
        throw new RuntimeException("Failed to write summary JSON to file", e);
    }
}
