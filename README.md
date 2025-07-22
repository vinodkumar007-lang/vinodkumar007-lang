private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorReportMap
) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        // Clean URL and status handling
        String blobUrl = file.getBlobUrl();
        String status = (blobUrl == null || blobUrl.isEmpty()) ? "" : file.getStatus();

        switch (file.getOutputType()) {
            case "EMAIL":
                entry.setEmailBlobUrl(blobUrl);
                entry.setEmailStatus(status);
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(blobUrl);
                entry.setArchiveStatus(status);
                break;
            case "PRINT":
                entry.setPrintBlobUrl(blobUrl);
                entry.setPrintStatus(status);
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(blobUrl);
                entry.setMobstatStatus(status);
                break;
        }

        grouped.put(key, entry);
    }

    // Finalize status per customer
    for (ProcessedFileEntry entry : grouped.values()) {
        boolean archiveSuccess = "SUCCESS".equalsIgnoreCase(entry.getArchiveStatus());

        // For EMAIL/PRINT/MOBSTAT, consider failed if status empty and present in errorMap
        boolean emailFailed = isFailedOrMissing(entry.getEmailStatus(), entry.getCustomerId(), entry.getAccountNumber(), "EMAIL", errorReportMap);
        boolean printFailed = isFailedOrMissing(entry.getPrintStatus(), entry.getCustomerId(), entry.getAccountNumber(), "PRINT", errorReportMap);
        boolean mobstatFailed = isFailedOrMissing(entry.getMobstatStatus(), entry.getCustomerId(), entry.getAccountNumber(), "MOBSTAT", errorReportMap);

        boolean anyFailed = emailFailed || printFailed || mobstatFailed;

        // Ensure ARCHIVE is always retained even if it's the only one
        if (!archiveSuccess) {
            entry.setOverallStatus("FAILED");
        } else if (anyFailed) {
            entry.setOverallStatus("FAILED"); // You can switch to PARTIAL if desired
        } else {
            entry.setOverallStatus("SUCCESS");
        }

        // Force empty status to "" if blobUrl is also empty
        if (entry.getEmailBlobUrl() == null || entry.getEmailBlobUrl().isEmpty()) {
            entry.setEmailStatus("");
        }
        if (entry.getPrintBlobUrl() == null || entry.getPrintBlobUrl().isEmpty()) {
            entry.setPrintStatus("");
        }
        if (entry.getMobstatBlobUrl() == null || entry.getMobstatBlobUrl().isEmpty()) {
            entry.setMobstatStatus("");
        }
    }

    return new ArrayList<>(grouped.values());
}

==========

private static boolean isFailedOrMissing(
        String status,
        String customerId,
        String accountNumber,
        String method,
        Map<String, Map<String, String>> errorReportMap
) {
    if (status == null || status.isEmpty()) {
        String key = customerId + "|" + accountNumber;
        Map<String, String> methodStatus = errorReportMap.getOrDefault(key, new HashMap<>());
        String error = methodStatus.get(method);
        return "Failed".equalsIgnoreCase(error);
    }
    return "FAILED".equalsIgnoreCase(status);
}
