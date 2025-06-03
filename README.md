// Get the JSON content string
String jsonContent = SummaryJsonWriter.writeSummaryJson(summaryPayload);

logger.info("📄 Summary JSON content before upload:\n{}", jsonContent);

// If you want to save it to a file, do this:
String summaryFilePath = "summary.json"; // or a dynamic path you want

try {
    java.nio.file.Files.write(java.nio.file.Paths.get(summaryFilePath), jsonContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    logger.info("✅ Summary JSON written to file: {}", summaryFilePath);
} catch (IOException e) {
    logger.warn("⚠️ Could not write summary.json file", e);
}
