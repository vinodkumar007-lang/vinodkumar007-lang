 // Write summary JSON file and upload it
        String summaryJsonPath = SummaryJsonWriter.writeSummaryJson(summaryPayload);
        summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message);

  public static String writeSummaryJson(SummaryPayload payload) {
        try {
            StringWriter writer = new StringWriter();
            objectMapper.writeValue(writer, payload);
            logger.info("✅ Summary JSON successfully written.");
            return writer.toString();
        } catch (Exception e) {
            logger.error("❌ Failed to write summary JSON: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to write summary JSON", e);
        }
    }
