String jsonContent = SummaryJsonWriter.writeSummaryJson(summaryPayload);

// Optional: log before saving
logger.info("📄 Summary JSON content before upload:\n{}", jsonContent);

// Save to a file
String filePath = "summary.json"; // or dynamic name based on batchID
try {
    Files.write(Paths.get(filePath), jsonContent.getBytes(StandardCharsets.UTF_8));
    logger.info("✅ Summary JSON written to file: {}", filePath);
} catch (IOException e) {
    logger.error("❌ Failed to write summary JSON to file", e);
}
