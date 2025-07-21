private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
    Map<String, Map<String, String>> errorMap = new HashMap<>();

    try {
        Optional<Path> errorReportPath = findFilePath(msg, "ErrorReport.csv");
        if (errorReportPath.isEmpty()) {
            logger.info("⚠️ No ErrorReport.csv found for batch ID {}", msg.getBatchId());
            return errorMap; // Return empty map
        }

        Path path = errorReportPath.get();
        logger.info("📄 Parsing ErrorReport from: {}", path.toString());

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split("\\|");

                String acc = null;
                String method = null;
                String status = null;

                if (parts.length >= 5) {
                    // Format: acc|accNum|some_field|method|status
                    acc = parts[0].trim();
                    method = parts[3].trim().toUpperCase();
                    status = parts[4].trim();
                } else if (parts.length == 4) {
                    // Format: acc|accNum|method|status
                    acc = parts[0].trim();
                    method = parts[2].trim().toUpperCase();
                    status = parts[3].trim();
                } else {
                    logger.warn("❗ Skipping malformed ErrorReport line: {}", line);
                    continue;
                }

                if (acc != null && method != null && status != null) {
                    errorMap
                        .computeIfAbsent(acc, k -> new HashMap<>())
                        .put(method, status);
                }
            }
        }

        logger.info("✅ Parsed ErrorReport.csv: {} customer entries", errorMap.size());
        errorMap.forEach((acc, methods) ->
            logger.debug("➡️ Account: {}, Error Methods: {}", acc, methods)
        );

    } catch (IOException e) {
        logger.error("❌ Failed to parse ErrorReport.csv for batch {}: {}", msg.getBatchId(), e.getMessage(), e);
    }

    return errorMap;
}

