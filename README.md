 // 4. Write summary.json locally
        File summaryJsonFile = new File(SummaryJsonWriter.writeSummaryJson(summaryPayload));

        // 5. Upload summary.json to Azure Blob Storage, get URL
        summaryFileUrl = blobStorageService.uploadFile(summaryJsonFile.getAbsolutePath(),
                buildSummaryJsonBlobPath(message));
