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

        if (outputMethod != null) {
            switch (outputMethod.trim().toUpperCase()) {
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
        }

        grouped.put(key, entry);
    }

    for (ProcessedFileEntry entry : grouped.values()) {
        String overallStatus = determineOverallStatus(entry, errorMap);
        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}
