 File summaryFilePath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload, "/temp/summary.json");
        File summaryJsonFile = new File(summaryFilePath);

        try {
            String jsonContent = new String(java.nio.file.Files.readAllBytes(summaryJsonFile.toPath()));
            logger.info("📄 Summary JSON content before upload:\n{}", jsonContent);
        } catch (IOException e) {
            logger.warn("⚠️ Could not read summary.json for logging", e);
        }

        summaryFileUrl = blobStorageService.uploadFile(summaryFilePath, buildSummaryJsonBlobPath(message));
