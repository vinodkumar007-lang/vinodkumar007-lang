private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String method = file.getMethod();
        String status = file.getStatus();
        String url = file.getBlobUrl();

        if ("EMAIL".equalsIgnoreCase(method)) {
            entry.setEmailStatus(status);
            entry.setEmailUrl(url);
        } else if ("PRINT".equalsIgnoreCase(method)) {
            entry.setPrintStatus(status);
            entry.setPrintUrl(url);
        } else if ("MOBSTAT".equalsIgnoreCase(method)) {
            entry.setMobstatStatus(status);
            entry.setMobstatUrl(url);
        } else if ("ARCHIVE".equalsIgnoreCase(method)) {
            entry.setArchiveStatus(status);
            entry.setArchiveUrl(url);
        }

        grouped.put(key, entry);
    }

    // Step 2: Handle errorMap-based statuses (NOT_FOUND or FAILED)
    for (Map.Entry<String, Map<String, String>> errorEntry : errorMap.entrySet()) {
        String key = errorEntry.getKey();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(key.split("-")[0]);
        entry.setAccountNumber(key.split("-")[1]);

        Map<String, String> methodStatusMap = errorEntry.getValue();
        for (Map.Entry<String, String> m : methodStatusMap.entrySet()) {
            String method = m.getKey();
            String errStatus = m.getValue(); // "NOT_FOUND" or "FAILED"

            if ("EMAIL".equalsIgnoreCase(method) && entry.getEmailStatus() == null) {
                entry.setEmailStatus(errStatus);
            } else if ("PRINT".equalsIgnoreCase(method) && entry.getPrintStatus() == null) {
                entry.setPrintStatus(errStatus);
            } else if ("MOBSTAT".equalsIgnoreCase(method) && entry.getMobstatStatus() == null) {
                entry.setMobstatStatus(errStatus);
            } else if ("ARCHIVE".equalsIgnoreCase(method) && entry.getArchiveStatus() == null) {
                entry.setArchiveStatus(errStatus);
            }
        }

        grouped.put(key, entry);
    }

    // Step 3: Fill in "NA" where statuses are still null
    for (ProcessedFileEntry entry : grouped.values()) {
        if (entry.getEmailStatus() == null) entry.setEmailStatus("NA");
        if (entry.getPrintStatus() == null) entry.setPrintStatus("NA");
        if (entry.getMobstatStatus() == null) entry.setMobstatStatus("NA");
        if (entry.getArchiveStatus() == null) entry.setArchiveStatus("NA");
    }

    // Step 4: Calculate overallStatus
    for (ProcessedFileEntry entry : grouped.values()) {
        String email = entry.getEmailStatus();
        String print = entry.getPrintStatus();
        String mobstat = entry.getMobstatStatus();
        String archive = entry.getArchiveStatus();

        boolean isEmailSuccess = "SUCCESS".equals(email);
        boolean isPrintSuccess = "SUCCESS".equals(print);
        boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
        boolean isArchiveSuccess = "SUCCESS".equals(archive);

        boolean isEmailNA = "NA".equals(email);
        boolean isPrintNA = "NA".equals(print);
        boolean isMobstatNA = "NA".equals(mobstat);

        boolean isEmailMissingOrFailed = "FAILED".equals(email) || "NOT_FOUND".equals(email);
        boolean isPrintMissingOrFailed = "FAILED".equals(print) || "NOT_FOUND".equals(print);
        boolean isMobstatMissingOrFailed = "FAILED".equals(mobstat) || "NOT_FOUND".equals(mobstat);

        if (isArchiveSuccess &&
            (isEmailSuccess || isEmailNA) &&
            (isPrintSuccess || isPrintNA) &&
            (isMobstatSuccess || isMobstatNA)) {
            entry.setOverallStatus("SUCCESS");
        } else if (isArchiveSuccess &&
                (isEmailMissingOrFailed || isPrintMissingOrFailed || isMobstatMissingOrFailed)) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(grouped.values());
}
