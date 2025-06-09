public static String writeSummaryJsonToFile(SummaryPayload payload) {
    try {
        // Prepare clean filename
        String fileName = "summary_" + payload.getBatchID() + ".json";
        
        // Create a temp directory to store summary files (optional, you can change folder)
        Path tempDir = Files.createTempDirectory("summaryFiles");
        
        // Full path with clean filename
        Path filePath = tempDir.resolve(fileName);
        
        // Write JSON to file
        objectMapper.writeValue(filePath.toFile(), payload);
        
        logger.info("✅ Summary JSON successfully written to file: {}", filePath);
        return filePath.toString();
    } catch (Exception e) {
        logger.error("❌ Failed to write summary JSON to file: {}", e.getMessage(), e);
        throw new RuntimeException("Failed to write summary JSON to file", e);
    }
}
