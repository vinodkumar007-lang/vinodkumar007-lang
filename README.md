// Write the summary JSON file
String summaryJsonPath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload);

// Extract just the file name (not full path) to keep blob name clean
String summaryFileName = new File(summaryJsonPath).getName();

// Upload using the clean file name
String summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message, summaryFileName);
