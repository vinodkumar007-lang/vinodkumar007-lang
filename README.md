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

        // Set individual statuses
        if (isNonEmpty(entry.getEmailBlobUrl())) {
            entry.setEmailStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("EMAIL")) {
            entry.setEmailStatus("FAILED");
        } else {
            entry.setEmailStatus("");
        }

        if (isNonEmpty(entry.getArchiveBlobUrl())) {
            entry.setArchiveStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("ARCHIVE")) {
            entry.setArchiveStatus("FAILED");
        } else {
            entry.setArchiveStatus("");
        }

        if (isNonEmpty(entry.getPrintBlobUrl())) {
            entry.setPrintStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("PRINT")) {
            entry.setPrintStatus("FAILED");
        } else {
            entry.setPrintStatus("");
        }

        if (isNonEmpty(entry.getMobstatBlobUrl())) {
            entry.setMobstatStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("MOBSTAT")) {
            entry.setMobstatStatus("FAILED");
        } else {
            entry.setMobstatStatus("");
        }

        // ✅ Step 1: EMAIL found + ARCHIVE found = SUCCESS
        if (isNonEmpty(entry.getEmailBlobUrl()) &&
            isNonEmpty(entry.getArchiveBlobUrl())) {
            entry.setOverallStatus("SUCCESS");
            continue;
        }

        // ✅ Step 2 & 3: EMAIL missing but ARCHIVE success = PARTIAL (based on errorMap or empty)
        if (!isNonEmpty(entry.getEmailBlobUrl()) &&
            isNonEmpty(entry.getArchiveBlobUrl())) {

            if (errorMapForKey.containsKey("EMAIL") || !errorMapForKey.containsKey("EMAIL")) {
                entry.setEmailStatus("FAILED"); // explicitly mark as failed
                entry.setOverallStatus("PARTIAL");
                continue;
            }
        }

        // ✅ ErrorReport: If no delivery method found at all, but archive is present
        if (!isNonEmpty(entry.getEmailBlobUrl()) &&
            !isNonEmpty(entry.getPrintBlobUrl()) &&
            !isNonEmpty(entry.getMobstatBlobUrl()) &&
            isNonEmpty(entry.getArchiveBlobUrl()) &&
            errorMapForKey.isEmpty()) {

            entry.setEmailStatus("FAILED");
            entry.setPrintStatus("FAILED");
            entry.setMobstatStatus("FAILED");
            entry.setOverallStatus("PARTIAL");
            continue;
        }

        // Default fallback using existing status logic
        entry.setOverallStatus(determineOverallStatus(entry));
    }

    return new ArrayList<>(grouped.values());
}

private static boolean isNonEmpty(String val) {
    return val != null && !val.trim().isEmpty();
}

private static String safeStatus(String status) {
    return status != null ? status.trim().toUpperCase() : "";
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
            notFoundCount++;
        }
    }

    if (successCount == 4) return "SUCCESS";
    if (failedCount > 0 && successCount == 0) return "FAILED";
    if (successCount > 0 && (failedCount > 0 || notFoundCount > 0)) return "PARTIAL";
    if (notFoundCount == 4) return "FAILED";
    return "PARTIAL";
}
