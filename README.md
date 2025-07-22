private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap
) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String customerId = file.getCustomerId();
        String accountNumber = file.getAccountNumber();

        if (customerId == null || accountNumber == null) continue;

        String key = (customerId + "-" + accountNumber).trim().toUpperCase();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);

        String outputType = file.getOutputType();
        if (outputType == null) continue;

        String normalizedType = outputType.trim().toUpperCase();
        String blobUrl = file.getBlobUrl();
        String status = file.getStatus();

        switch (normalizedType) {
            case "EMAIL":
                if (blobUrl != null) {
                    entry.setEmailUrl(blobUrl);
                    entry.setEmailStatus(status);
                }
                break;
            case "ARCHIVE":
                if (blobUrl != null) {
                    entry.setArchiveUrl(blobUrl);
                    entry.setArchiveStatus(status);
                }
                break;
            case "PRINT":
                if (blobUrl != null) {
                    entry.setPrintUrl(blobUrl);
                    entry.setPrintStatus(status);
                }
                break;
            case "MOBSTAT":
                if (blobUrl != null) {
                    entry.setMobstatUrl(blobUrl);
                    entry.setMobstatStatus(status);
                }
                break;
        }

        grouped.put(key, entry);
    }

    // Compute overallStatus
    for (ProcessedFileEntry entry : grouped.values()) {
        boolean emailSuccess = "SUCCESS".equalsIgnoreCase(entry.getEmailStatus());
        boolean archiveSuccess = "SUCCESS".equalsIgnoreCase(entry.getArchiveStatus());

        boolean emailExists = entry.getEmailUrl() != null;
        boolean archiveExists = entry.getArchiveUrl() != null;

        String emailStatus = entry.getEmailStatus();

        boolean isEmailFailedInErrorMap = false;
        Map<String, String> customerError = errorMap.getOrDefault(entry.getCustomerId(), new HashMap<>());
        if ("FAILED".equalsIgnoreCase(emailStatus)) {
            isEmailFailedInErrorMap = customerError.containsKey(entry.getAccountNumber());
        }

        if (emailSuccess && archiveSuccess) {
            entry.setOverallStatus("SUCCESS");
        } else if (!emailExists && archiveSuccess && !isEmailFailedInErrorMap) {
            entry.setOverallStatus("SUCCESS");
        } else if ("FAILED".equalsIgnoreCase(emailStatus) && archiveSuccess) {
            entry.setOverallStatus("FAILED");
        } else if (emailExists && "SUCCESS".equalsIgnoreCase(emailStatus) && !"SUCCESS".equalsIgnoreCase(entry.getArchiveStatus())) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(grouped.values());
}
