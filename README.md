private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();

        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputType = file.getOutputType() != null ? file.getOutputType().toUpperCase(Locale.ROOT) : "";
        String blobUrl = file.getBlobUrl();
        String status = file.getStatus();

        System.out.println("Processing file for key: " + key + " | outputType: " + outputType + " | blobUrl: " + blobUrl + " | status: " + status);

        switch (outputType) {
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

    // Final pass: calculate overall status
    for (Map.Entry<String, ProcessedFileEntry> mapEntry : grouped.entrySet()) {
        ProcessedFileEntry entry = mapEntry.getValue();

        String errorKey = entry.getCustomerId() + "-" + entry.getAccountNumber();
        boolean isErrorPresent = errorMap.containsKey(errorKey);

        String emailStatus = entry.getEmailStatus();
        String archiveStatus = entry.getArchiveStatus();

        String overallStatus;

        if (isErrorPresent) {
            overallStatus = "FAILED";
        } else if ("SUCCESS".equalsIgnoreCase(emailStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
            overallStatus = "SUCCESS";
        } else if ("SUCCESS".equalsIgnoreCase(archiveStatus) && (emailStatus == null || emailStatus.isEmpty())) {
            overallStatus = "SUCCESS";
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
