private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap) {

        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();
            ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            // Initialize outputType blob URLs
            if ("EMAIL".equalsIgnoreCase(file.getOutputType())) {
                entry.setEmailBlobUrl(file.getBlobUrl());
            } else if ("PRINT".equalsIgnoreCase(file.getOutputType())) {
                entry.setPrintBlobUrl(file.getBlobUrl());
            } else if ("MOBSTAT".equalsIgnoreCase(file.getOutputType())) {
                entry.setMobstatBlobUrl(file.getBlobUrl());
            } else if ("ARCHIVE".equalsIgnoreCase(file.getOutputType())) {
                entry.setArchiveBlobUrl(file.getBlobUrl());
            }

            grouped.put(key, entry);
        }

        // Evaluate statuses
        for (Map.Entry<String, ProcessedFileEntry> e : grouped.entrySet()) {
            String key = e.getKey();
            ProcessedFileEntry entry = e.getValue();
            Map<String, String> methodErrors = errorMap.getOrDefault(key, Collections.emptyMap());

            // EMAIL
            if (isNonEmpty(entry.getEmailBlobUrl())) {
                entry.setEmailStatus("SUCCESS");
            } else {
                String error = methodErrors.getOrDefault("EMAIL", "");
                entry.setEmailStatus("FAILED".equalsIgnoreCase(error) ? "FAILED" : "NOT-FOUND");
            }

            // PRINT
            if (isNonEmpty(entry.getPrintBlobUrl())) {
                entry.setPrintStatus("SUCCESS");
            } else {
                String error = methodErrors.getOrDefault("PRINT", "");
                entry.setPrintStatus("FAILED".equalsIgnoreCase(error) ? "FAILED" : "NOT-FOUND");
            }

            // MOBSTAT
            if (isNonEmpty(entry.getMobstatBlobUrl())) {
                entry.setMobstatStatus("SUCCESS");
            } else {
                String error = methodErrors.getOrDefault("MOBSTAT", "");
                entry.setMobstatStatus("FAILED".equalsIgnoreCase(error) ? "FAILED" : "NOT-FOUND");
            }

            // ARCHIVE (always present according to spec)
            entry.setArchiveStatus(isNonEmpty(entry.getArchiveBlobUrl()) ? "SUCCESS" : "FAILED");

            // Compute overall status
            entry.setOverallStatus(determineOverallStatus(entry));
        }

        return new ArrayList<>(grouped.values());
    }
    private static String determineOverallStatus(ProcessedFileEntry entry) {
        String email = entry.getEmailStatus();
        String print = entry.getPrintStatus();
        String mobstat = entry.getMobstatStatus();
        String archive = entry.getArchiveStatus();

        int successCount = 0;
        int failedCount = 0;
        int notFoundCount = 0;

        List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);
        for (String status : allStatuses) {
            if ("SUCCESS".equalsIgnoreCase(status)) {
                successCount++;
            } else if ("FAILED".equalsIgnoreCase(status)) {
                failedCount++;
            } else if ("NOT-FOUND".equalsIgnoreCase(status) || status == null || status.trim().isEmpty()) {
                notFoundCount++;
            }
        }

        // ✅ All 4 methods successful
        if (successCount == 4) {
            return "SUCCESS";
        }

        // ❌ At least one failed, none success
        if (failedCount > 0 && successCount == 0) {
            return "FAILED";
        }

        // ⚠️ At least one success, and at least one failed or not-found
        if (successCount > 0 && (failedCount > 0 || notFoundCount > 0)) {
            return "PARTIAL";
        }

        // ❌ All methods are NOT-FOUND (no outputs generated)
        if (notFoundCount == 4) {
            return "FAILED";
        }

        // Fallback safety
        return "PARTIAL";
    }
