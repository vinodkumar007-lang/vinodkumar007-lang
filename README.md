private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
        Map<String, Map<String, String>> map = new HashMap<>();
        Path errorPath = Paths.get(mountPath, "output", msg.getSourceSystem(), msg.getJobName(), "ErrorReport.csv");

        if (!Files.exists(errorPath)) return map;

        try (BufferedReader reader = Files.newBufferedReader(errorPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 5) {
                    String acc = parts[0].trim();
                    String method = parts[3].trim().toUpperCase();
                    String status = parts[4].trim();
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                } else if (parts.length >= 3) {
                    String acc = parts[0].trim();
                    String method = parts[2].trim().toUpperCase();
                    String status = parts.length > 3 ? parts[3].trim() : "Failed";
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                }
            }
        } catch (Exception e) {
            logger.warn("⚠️ Error reading ErrorReport.csv", e);
        }
        return map;
    }
