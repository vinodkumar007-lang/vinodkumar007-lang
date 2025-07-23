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
                entry.setEmailBlobUrl(blobUrl);
                break;
            case "PRINT":
                entry.setPrintStatus(status);
                entry.setPrintFileUrl(blobUrl);
                break;
            case "MOBSTAT":
                entry.setMobstatStatus(status);
                entry.setMobstatBlobUrl(blobUrl);
                break;
            case "ARCHIVE":
                entry.setArchiveStatus(status);
                entry.setArchiveBlobUrl(blobUrl);
                break;
        }

        grouped.put(key, entry);
    }

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

    List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);

    // ✅ Rule: if any status is FAILED → overall FAILED
    boolean hasFailed = allStatuses.stream().anyMatch(s -> "FAILED".equalsIgnoreCase(s));
    if (hasFailed) {
        return "FAILED";
    }

    // ✅ Generic rule: if at least two SUCCESS and all statuses present → SUCCESS
    long nonBlankCount = allStatuses.stream().filter(s -> !s.isEmpty()).count();
    long successCount = allStatuses.stream().filter(s -> "SUCCESS".equals(s)).count();
    if (successCount >= 2 && successCount == nonBlankCount) {
        return "SUCCESS";
    }

    // ✅ Check errorMap → if error present for missing output method → PARTIAL
    Map<String, String> errorEntries = errorMap.getOrDefault(customerId + "-" + accountNumber, Collections.emptyMap());
    for (Map.Entry<String, String> e : errorEntries.entrySet()) {
        String method = e.getKey();
        if (("EMAIL".equalsIgnoreCase(method) && email.isEmpty())
                || ("PRINT".equalsIgnoreCase(method) && print.isEmpty())
                || ("MOBSTAT".equalsIgnoreCase(method) && mobstat.isEmpty())) {
            return "PARTIAL";
        }
    }

    // ✅ Special rule: archive SUCCESS and everything else blank → SUCCESS
    if ("SUCCESS".equals(archive)
            && email.isEmpty() && print.isEmpty() && mobstat.isEmpty()) {
        return "SUCCESS";
    }

    // ✅ If any method is missing (empty) but not in error map → PARTIAL
    boolean anyMissing = allStatuses.stream().anyMatch(s -> s.isEmpty());
    if (anyMissing) {
        return "PARTIAL";
    }

    // ✅ Fallback
    return "PARTIAL";
}

private static String safeStatus(String status) {
    return status != null ? status.trim().toUpperCase() : "";
}
