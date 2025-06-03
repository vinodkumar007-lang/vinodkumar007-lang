// 4. Write summary.json locally and get the file path
String summaryFilePath = SummaryJsonWriter.writeSummaryJson(summaryPayload);
File summaryJsonFile = new File(summaryFilePath);

// ✅ Print the contents of the summary.json file
try {
    String jsonContent = new String(java.nio.file.Files.readAllBytes(summaryJsonFile.toPath()));
    logger.info("📄 Summary JSON content before upload:\n{}", jsonContent);
} catch (IOException e) {
    logger.warn("⚠️ Could not read summary.json for logging", e);
}

// 5. Upload summary.json to Azure Blob Storage
summaryFileUrl = blobStorageService.uploadFile(
        summaryFilePath,
        buildSummaryJsonBlobPath(message));
