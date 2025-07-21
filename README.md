private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
        Map<String, Map<String, String>> map = new HashMap<>();
        Path errorPath = Paths.get(mountPath, "output", msg.getSourceSystem(), msg.getJobName(), "ErrorReport.csv");

        if (!Files.exists(errorPath)) return map;

        try (BufferedReader reader = Files.newBufferedReader(errorPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");

                // Standardize logic for >= 3 fields
                if (parts.length >= 3) {
                    String acc = parts[0].trim();
                    String method = (parts.length >= 4 ? parts[3] : parts[2]).trim().toUpperCase();
                    String status = (parts.length >= 5 ? parts[4] : (parts.length >= 4 ? parts[3] : "Failed")).trim();

                    // Normalize missing/misplaced fields
                    if (method.isEmpty()) method = "UNKNOWN";
                    if (status.isEmpty()) status = "Failed";

                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                }
            }
        } catch (Exception e) {
            logger.warn("⚠️ Error reading ErrorReport.csv", e);
        }

        return map;
    }
