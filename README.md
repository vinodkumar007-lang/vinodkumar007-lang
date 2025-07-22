private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();

        ProcessedFileEntry entry;
        if (grouped.containsKey(key)) {
            entry = grouped.get(key); // retrieve existing
        } else {
            entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());
            grouped.put(key, entry);
        }

        String outputType = file.getOutputType().toUpperCase();
        String blobUrl = file.getBlobUrl() != null ? file.getBlobUrl() : "";

        switch (outputType) {
            case "EMAIL":
                entry.setEmailBlobUrl(blobUrl);
                entry.setEmailStatus(!blobUrl.isEmpty() ? "SUCCESS" : "");
                break;
            case "PRINT":
                entry.setPrintBlobUrl(blobUrl);
                entry.setPrintStatus(!blobUrl.isEmpty() ? "SUCCESS" : "");
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(blobUrl);
                entry.setMobstatStatus(!blobUrl.isEmpty() ? "SUCCESS" : "");
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(blobUrl);
                entry.setArchiveStatus(!blobUrl.isEmpty() ? "SUCCESS" : "FAILED"); // archive mandatory
                break;
        }
    }

    for (ProcessedFileEntry entry : grouped.values()) {
        String key = entry.getCustomerId() + "-" + entry.getAccountNumber();
        Map<String, String> errorStatusMap = errorMap.getOrDefault(key, new HashMap<>());

        if ((entry.getEmailBlobUrl() == null || entry.getEmailBlobUrl().isEmpty())
                && "FAILED".equalsIgnoreCase(errorStatusMap.get("EMAIL"))) {
            entry.setEmailStatus("FAILED");
        }

        if ((entry.getPrintBlobUrl() == null || entry.getPrintBlobUrl().isEmpty())
                && "FAILED".equalsIgnoreCase(errorStatusMap.get("PRINT"))) {
            entry.setPrintStatus("FAILED");
        }

        if ((entry.getMobstatBlobUrl() == null || entry.getMobstatBlobUrl().isEmpty())
                && "FAILED".equalsIgnoreCase(errorStatusMap.get("MOBSTAT"))) {
            entry.setMobstatStatus("FAILED");
        }

        // Determine overall status
        boolean allEmpty = Stream.of(
                entry.getEmailStatus(),
                entry.getPrintStatus(),
                entry.getMobstatStatus()
        ).allMatch(s -> s == null || s.isEmpty());

        boolean allSuccess = Stream.of(
                entry.getEmailStatus(),
                entry.getPrintStatus(),
                entry.getMobstatStatus()
        ).allMatch(s -> "SUCCESS".equalsIgnoreCase(s) || s == null || s.isEmpty());

        boolean anyFailed = Stream.of(
                entry.getEmailStatus(),
                entry.getPrintStatus(),
                entry.getMobstatStatus()
        ).anyMatch(s -> "FAILED".equalsIgnoreCase(s));

        if (allEmpty || allSuccess) {
            entry.setOverallStatus("SUCCESS");
        } else if (anyFailed && !allEmpty) {
            entry.setOverallStatus("FAILED");
        } else {
            entry.setOverallStatus("PARTIAL");
        }
    }

    return new ArrayList<>(grouped.values());
}
