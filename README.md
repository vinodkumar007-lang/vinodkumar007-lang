private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputType = file.getOutputType();
        String blobUrl = file.getBlobUrl();

        switch (outputType) {
            case "EMAIL":
                if (!isNonEmpty(entry.getEmailBlobUrl()) && isNonEmpty(blobUrl)) {
                    entry.setEmailBlobUrl(blobUrl);
                }
                break;
            case "ARCHIVE":
                if (!isNonEmpty(entry.getArchiveBlobUrl()) && isNonEmpty(blobUrl)) {
                    entry.setArchiveBlobUrl(blobUrl);
                }
                break;
            case "PRINT":
                if (!isNonEmpty(entry.getPrintBlobUrl()) && isNonEmpty(blobUrl)) {
                    entry.setPrintBlobUrl(blobUrl);
                }
                break;
            case "MOBSTAT":
                if (!isNonEmpty(entry.getMobstatBlobUrl()) && isNonEmpty(blobUrl)) {
                    entry.setMobstatBlobUrl(blobUrl);
                }
                break;
        }

        grouped.put(key, entry);
    }

    for (Map.Entry<String, ProcessedFileEntry> group : grouped.entrySet()) {
        String key = group.getKey();
        ProcessedFileEntry entry = group.getValue();
        Map<String, String> errorMapForKey = errorMap.getOrDefault(key, Collections.emptyMap());

        // EMAIL
        if (isNonEmpty(entry.getEmailBlobUrl())) {
            entry.setEmailStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("EMAIL")) {
            entry.setEmailStatus("FAILED");
        } else {
            entry.setEmailStatus("");
        }

        // ARCHIVE
        if (isNonEmpty(entry.getArchiveBlobUrl())) {
            entry.setArchiveStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("ARCHIVE")) {
            entry.setArchiveStatus("FAILED");
        } else {
            entry.setArchiveStatus("");
        }

        // PRINT
        if (isNonEmpty(entry.getPrintBlobUrl())) {
            entry.setPrintStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("PRINT")) {
            entry.setPrintStatus("FAILED");
        } else {
            entry.setPrintStatus("");
        }

        // MOBSTAT
        if (isNonEmpty(entry.getMobstatBlobUrl())) {
            entry.setMobstatStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("MOBSTAT")) {
            entry.setMobstatStatus("FAILED");
        } else {
            entry.setMobstatStatus("");
        }

        // ✅ FIX: Set overall status
        entry.setOverallStatus(determineOverallStatus(entry));
    }

    return new ArrayList<>(grouped.values());
}
