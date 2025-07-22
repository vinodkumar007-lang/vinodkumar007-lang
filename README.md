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
================
private static boolean isNonEmpty(String str) {
    return str != null && !str.trim().isEmpty();
}
