private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputMethod = file.getOutputMethod();
        String status = file.getStatus();
        String blobUrl = file.getBlobUrl();

        switch (outputMethod.toUpperCase()) {
            case "EMAIL":
                entry.setEmailStatus(status);
                entry.setEmailUrl(blobUrl);
                break;
            case "PRINT":
                entry.setPrintStatus(status);
                entry.setPrintUrl(blobUrl);
                break;
            case "MOBSTAT":
                entry.setMobstatStatus(status);
                entry.setMobstatUrl(blobUrl);
                break;
            case "ARCHIVE":
                entry.setArchiveStatus(status);
                entry.setArchiveUrl(blobUrl);
                break;
        }

        grouped.put(key, entry);
    }

    // Now determine overallStatus for each entry
    for (ProcessedFileEntry entry : grouped.values()) {
        String overallStatus = determineOverallStatus(entry, errorMap);
        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}

private static String determineOverallStatus(ProcessedFileEntry entry, Map<String, Map<String, String>> errorMap) {
    String email = safeStatus(entry.getEmailStatus());
    String print = safeStatus(entry.getPrintStatus());
    String mobstat = safeStatus(entry.getMobstatStatus());
    String archive = safeStatus(entry.getArchiveStatus());

    String customerId = entry.getCustomerId();
    String accountNumber = entry.getAccountNumber();

    boolean hasFailed = false;
    boolean hasSuccess = false;

    List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);
    for (String status : allStatuses) {
        if ("SUCCESS".equals(status)) hasSuccess = true;
        if ("FAILED".equals(status)) hasFailed = true;
    }

    // Generic check: if at least two output methods have SUCCESS, and rest are "", it's SUCCESS
    long nonBlankCount = allStatuses.stream().filter(s -> !s.isEmpty()).count();
    long successCount = allStatuses.stream().filter(s -> "SUCCESS".equals(s)).count();
    if (successCount >= 2 && successCount == nonBlankCount) {
        return "SUCCESS";
    }

    // If any method is FAILED, it's at least PARTIAL
    if (hasFailed) {
        return "PARTIAL";
    }

    // Check errorMap for entries with same customer + account
    Map<String, String> errorEntries = errorMap.getOrDefault(customerId + "-" + accountNumber, Collections.emptyMap());

    for (Map.Entry<String, String> e : errorEntries.entrySet()) {
        String method = e.getKey();
        String error = e.getValue();
        if (("EMAIL".equalsIgnoreCase(method) && email.isEmpty())
                || ("PRINT".equalsIgnoreCase(method) && print.isEmpty())
                || ("MOBSTAT".equalsIgnoreCase(method) && mobstat.isEmpty())) {
            return "PARTIAL";
        }
    }

    // Default SUCCESS if archive is SUCCESS and other methods are empty
    if ("SUCCESS".equals(archive) &&
        email.isEmpty() && print.isEmpty() && mobstat.isEmpty()) {
        return "SUCCESS";
    }

    return "PARTIAL";
}

private static String safeStatus(String status) {
    return status != null ? status.trim().toUpperCase() : "";
}
