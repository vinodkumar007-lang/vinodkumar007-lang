private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles, Map<String, String> errorMap) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        switch (file.getOutputType().toUpperCase()) {
            case "EMAIL":
                entry.setEmailBlobUrl(file.getBlobURL());
                entry.setEmailStatus(file.getStatus());
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(file.getBlobURL());
                entry.setArchiveStatus(file.getStatus());
                break;
            case "PRINT":
                entry.setPrintBlobUrl(file.getBlobURL());
                entry.setPrintStatus(file.getStatus());
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(file.getBlobURL());
                entry.setMobstatStatus(file.getStatus());
                break;
        }

        grouped.put(key, entry);
    }

    // Set overallStatus for each grouped entry
    for (ProcessedFileEntry entry : grouped.values()) {
        String key = entry.getCustomerId() + "-" + entry.getAccountNumber();

        // If errorMap has this customer+account → mark FAILED
        if (errorMap.containsKey(key)) {
            entry.setOverallStatus("FAILED");
            continue;
        }

        // If archiveBlobUrl is present (successfully uploaded) → mark SUCCESS
        if (entry.getArchiveBlobUrl() != null && !entry.getArchiveBlobUrl().isEmpty()) {
            entry.setOverallStatus("SUCCESS");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(grouped.values());
}
