public static File writeSummaryJsonToFile(SummaryPayload payload, String filePath) {
    try {
        String jsonContent = writeSummaryJson(payload); // reuse the string method
        Path path = Paths.get(filePath);
        Files.writeString(path, jsonContent);
        logger.info("📁 Summary JSON written to file: {}", path);
        return path.toFile();
    } catch (Exception e) {
        logger.error("❌ Failed to write summary JSON to file: {}", e.getMessage(), e);
        throw new RuntimeException("Failed to write summary JSON to file", e);
    }
}

File summaryFile = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload, "/tmp/summary.json");  
