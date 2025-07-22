private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();

        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        // Attach the blobUrl + status based on output type
        String outputType = file.getOutputType();
        if ("EMAIL".equalsIgnoreCase(outputType)) {
            entry.setEmailBlobUrl(file.getBlobUrl());
            entry.setEmailStatus(file.getStatus());
        } else if ("ARCHIVE".equalsIgnoreCase(outputType)) {
            entry.setArchiveBlobUrl(file.getBlobUrl());
            entry.setArchiveStatus(file.getStatus());
        } else if ("PRINT".equalsIgnoreCase(outputType)) {
            entry.setPrintBlobUrl(file.getBlobUrl());
            entry.setPrintStatus(file.getStatus());
        } else if ("MOBSTAT".equalsIgnoreCase(outputType)) {
            entry.setMobstatBlobUrl(file.getBlobUrl());
            entry.setMobstatStatus(file.getStatus());
        }

        grouped.put(key, entry);
    }

    // Final pass to compute overallStatus
    for (Map.Entry<String, ProcessedFileEntry> mapEntry : grouped.entrySet()) {
        ProcessedFileEntry entry = mapEntry.getValue();

        String errorKey = entry.getCustomerId() + "-" + entry.getAccountNumber();
        boolean isErrorPresent = errorMap.containsKey(errorKey);

        String emailStatus = entry.getEmailStatus();
        String archiveStatus = entry.getArchiveStatus();

        String overallStatus;

        if ("SUCCESS".equalsIgnoreCase(emailStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
            overallStatus = "SUCCESS";
        } else if ("SUCCESS".equalsIgnoreCase(archiveStatus) && (emailStatus == null || emailStatus.isEmpty())) {
            overallStatus = isErrorPresent ? "FAILED" : "SUCCESS";
        } else if ("FAILED".equalsIgnoreCase(emailStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
            overallStatus = "FAILED";
        } else if ("SUCCESS".equalsIgnoreCase(emailStatus) && !"SUCCESS".equalsIgnoreCase(archiveStatus)) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "FAILED";
        }

        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}
