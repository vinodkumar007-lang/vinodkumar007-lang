private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();

        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String errorKey = file.getCustomerId() + "-" + file.getAccountNumber();

        // Set fields based on output type
        switch (file.getOutputType()) {
            case "EMAIL":
                entry.setEmailBlobUrl(file.getBlobUrl());
                entry.setEmailStatus(file.getStatus());
                break;

            case "ARCHIVE":
                entry.setArchiveBlobUrl(file.getBlobUrl());
                entry.setArchiveStatus(file.getStatus());
                break;

            case "PRINT":
                entry.setPrintBlobUrl(file.getBlobUrl());
                entry.setPrintStatus(file.getStatus());
                break;

            case "MOBSTAT":
                entry.setMobstatBlobUrl(file.getBlobUrl());
                entry.setMobstatStatus(file.getStatus());
                break;
        }

        grouped.put(key, entry); // Always put back to update
    }

    // Final pass to determine overallStatus
    for (ProcessedFileEntry entry : grouped.values()) {
        String errorKey = entry.getCustomerId() + "-" + entry.getAccountNumber();
        boolean isError = errorMap.containsKey(errorKey);

        String emailStatus = entry.getEmailStatus();
        String archiveStatus = entry.getArchiveStatus();

        String overallStatus = "SUCCESS";

        if ("SUCCESS".equalsIgnoreCase(emailStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
            overallStatus = "SUCCESS";
        } else if ("FAILED".equalsIgnoreCase(emailStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
            overallStatus = "FAILED";
        } else if ("SUCCESS".equalsIgnoreCase(emailStatus) && !"SUCCESS".equalsIgnoreCase(archiveStatus)) {
            overallStatus = "PARTIAL";
        } else if (emailStatus == null && archiveStatus != null && archiveStatus.equals("SUCCESS")) {
            if (isError) {
                overallStatus = "FAILED";
            } else {
                overallStatus = "SUCCESS"; // fallback if no email but no error reported
            }
        } else {
            overallStatus = "FAILED";
        }

        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}
