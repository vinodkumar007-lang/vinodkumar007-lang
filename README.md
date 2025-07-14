private List<SummaryProcessedFile> appendFailureEntries(String errorReportFilePath, Map<String, String> successMap) {
        List<SummaryProcessedFile> failures = new ArrayList<>();
        if (errorReportFilePath == null) return failures;
        Path path = Paths.get(errorReportFilePath);
        if (!Files.exists(path)) return failures;

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 2) {
                    String account = parts[0].trim();
                    String customer = parts[1].trim();
                    if (!successMap.containsKey(account)) {
                        SummaryProcessedFile failEntry = new SummaryProcessedFile();
                        failEntry.setAccountNumber(account);
                        failEntry.setCustomerId(customer);
                        failEntry.setStatusCode("FAILURE");
                        failEntry.setStatusDescription("Processing failed");
                        failures.add(failEntry);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("❌ Failed to read error report file", e);
        }
        return failures;
    }
