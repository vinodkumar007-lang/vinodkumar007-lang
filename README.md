printFile.setPrintFileURL(blobStorageService.buildPrintFileUrl(message));

summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message);
