String summaryFileName = "summary_" + message.getBatchId() + ".json";
String summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message, summaryFileName);
