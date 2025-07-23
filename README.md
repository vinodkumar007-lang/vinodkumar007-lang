    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap) {

        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();
            ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            String outputType = file.getOutputType();
            String blobUrl = file.getBlobUrl();

            switch (outputType) {
                case "EMAIL":
                    if (!isNonEmpty(entry.getEmailBlobUrl())) {
                        entry.setEmailBlobUrl(blobUrl);
                    }
                    break;
                case "ARCHIVE":
                    if (!isNonEmpty(entry.getArchiveBlobUrl())) {
                        entry.setArchiveBlobUrl(blobUrl);
                    }
                    break;
                case "PRINT":
                    if (!isNonEmpty(entry.getPrintBlobUrl())) {
                        entry.setPrintBlobUrl(blobUrl);
                    }
                    break;
                case "MOBSTAT":
                    if (!isNonEmpty(entry.getMobstatBlobUrl())) {
                        entry.setMobstatBlobUrl(blobUrl);
                    }
                    break;
            }

            grouped.put(key, entry);
        }

        for (Map.Entry<String, ProcessedFileEntry> group : grouped.entrySet()) {
            String key = group.getKey();
            ProcessedFileEntry entry = group.getValue();
            Map<String, String> errorMapForKey = errorMap.getOrDefault(key, Collections.emptyMap());

            // EMAIL
            if (isNonEmpty(entry.getEmailBlobUrl())) {
                entry.setEmailStatus("SUCCESS");
            } else if (errorMapForKey.containsKey("EMAIL")) {
                entry.setEmailStatus("FAILED");
            } else {
                entry.setEmailStatus("");
            }

            // ARCHIVE
            if (isNonEmpty(entry.getArchiveBlobUrl())) {
                entry.setArchiveStatus("SUCCESS");
            } else if (errorMapForKey.containsKey("ARCHIVE")) {
                entry.setArchiveStatus("FAILED");
            } else {
                entry.setArchiveStatus("");
            }

            // PRINT
            if (isNonEmpty(entry.getPrintBlobUrl())) {
                entry.setPrintStatus("SUCCESS");
            } else if (errorMapForKey.containsKey("PRINT")) {
                entry.setPrintStatus("FAILED");
            } else {
                entry.setPrintStatus("");
            }

            // MOBSTAT
            if (isNonEmpty(entry.getMobstatBlobUrl())) {
                entry.setMobstatStatus("SUCCESS");
            } else if (errorMapForKey.containsKey("MOBSTAT")) {
                entry.setMobstatStatus("FAILED");
            } else {
                entry.setMobstatStatus("");
            }

            // ✅ FIX: Set overall status
            entry.setOverallStatus(determineOverallStatus(entry));

            //determineOverallStatus(entry);
        }

        return new ArrayList<>(grouped.values());
    }

    private static boolean isNonEmpty(String val) {
        return val != null && !val.trim().isEmpty();
    }


    private static String determineOverallStatus(ProcessedFileEntry entry) {
        String email = safeStatus(entry.getEmailStatus());
        String print = safeStatus(entry.getPrintStatus());
        String mobstat = safeStatus(entry.getMobstatStatus());
        String archive = safeStatus(entry.getArchiveStatus());

        int successCount = 0;
        int failedCount = 0;
        int notFoundCount = 0;

        List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);
        for (String status : allStatuses) {
            if ("SUCCESS".equalsIgnoreCase(status)) {
                successCount++;
            } else if ("FAILED".equalsIgnoreCase(status)) {
                failedCount++;
            } else {
                // Covers "" or null or unknown statuses
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

        // ❌ All methods are not found or empty
        if (notFoundCount == 4) {
            return "FAILED";
        }

        // Fallback
        return "PARTIAL";
    }

    private static String safeStatus(String status) {
        return status != null ? status.trim().toUpperCase() : "";
    }
