public static String writeSummaryJsonToFile(SummaryPayload payload) {
        try {
            String fileName = "summary_" + payload.getBatchID() + ".json";
            Path tempFile = Files.createTempFile(payload.getBatchID() + "_" + fileName.replace(".json", ""), ".json");
            objectMapper.writeValue(tempFile.toFile(), payload);
            logger.info("✅ Summary JSON successfully written to file: {}", tempFile);
            return tempFile.toString();
        } catch (Exception e) {
            logger.error("❌ Failed to write summary JSON to file: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to write summary JSON to file", e);
        }
    }
