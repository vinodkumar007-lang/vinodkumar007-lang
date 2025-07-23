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

        // Avoid duplicate override
        switch (outputType) {
            case "EMAIL":
                if (!isNonEmpty(entry.getEmailBlobUrl())) {
                    entry.setEmailBlobUrl(blobUrl);
                }
                break;
            case "ARCHIVE":
                if (!isNonEmpty(entry.getArchiveBlobUrl())) {
                    entry.setArchiveBlobUrl(blobUrl);
                }
                break;
            case "PRINT":
                if (!isNonEmpty(entry.getPrintBlobUrl())) {
                    entry.setPrintBlobUrl(blobUrl);
                }
                break;
            case "MOBSTAT":
                if (!isNonEmpty(entry.getMobstatBlobUrl())) {
                    entry.setMobstatBlobUrl(blobUrl);
                }
                break;
        }

        grouped.put(key, entry);
    }

    for (Map.Entry<String, ProcessedFileEntry> group : grouped.entrySet()) {
        String key = group.getKey();
        ProcessedFileEntry entry = group.getValue();

        // EMAIL
        if (isNonEmpty(entry.getEmailBlobUrl())) {
            entry.setEmailStatus("SUCCESS");
        } else {
            String emailError = errorMap.getOrDefault(key, Collections.emptyMap()).get("EMAIL");
            entry.setEmailStatus(emailError != null ? "FAILED" : "NOT-FOUND");
        }

        // ARCHIVE
        if (isNonEmpty(entry.getArchiveBlobUrl())) {
            entry.setArchiveStatus("SUCCESS");
        } else {
            String archiveError = errorMap.getOrDefault(key, Collections.emptyMap()).get("ARCHIVE");
            entry.setArchiveStatus(archiveError != null ? "FAILED" : "NOT-FOUND");
        }

        // PRINT
        if (isNonEmpty(entry.getPrintBlobUrl())) {
            entry.setPrintStatus("SUCCESS");
        } else {
            String printError = errorMap.getOrDefault(key, Collections.emptyMap()).get("PRINT");
            entry.setPrintStatus(printError != null ? "FAILED" : "NOT-FOUND");
        }

        // MOBSTAT
        if (isNonEmpty(entry.getMobstatBlobUrl())) {
            entry.setMobstatStatus("SUCCESS");
        } else {
            String mobstatError = errorMap.getOrDefault(key, Collections.emptyMap()).get("MOBSTAT");
            entry.setMobstatStatus(mobstatError != null ? "FAILED" : "NOT-FOUND");
        }

        // Set overall status
        setOverallStatus(entry);
    }

    return new ArrayList<>(grouped.values());
}

private static boolean isNonEmpty(String val) {
    return val != null && !val.trim().isEmpty();
}
