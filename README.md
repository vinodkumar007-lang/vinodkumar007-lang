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

    // Final loop to calculate overallStatus
    for (ProcessedFileEntry entry : grouped.values()) {
        String errorKey = entry.getCustomerId() + "-" + entry.getAccountNumber();
        boolean isErrorPresent = errorMap.containsKey(errorKey);

        List<String> statuses = new ArrayList<>();
        if (entry.getEmailStatus() != null) statuses.add(entry.getEmailStatus());
        if (entry.getMobstatStatus() != null) statuses.add(entry.getMobstatStatus());
        if (entry.getPrintStatus() != null) statuses.add(entry.getPrintStatus());
        if (entry.getArchiveStatus() != null) statuses.add(entry.getArchiveStatus());

        boolean allSuccess = !statuses.isEmpty() && statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s));
        boolean anyFailed = statuses.stream().anyMatch(s -> "FAILED".equalsIgnoreCase(s));
        boolean allFailed = !statuses.isEmpty() && statuses.stream().allMatch(s -> "FAILED".equalsIgnoreCase(s));

        String overallStatus;

        if (allSuccess) {
            overallStatus = "SUCCESS";
        } else if (allFailed) {
            overallStatus = "FAILED";
        } else if (anyFailed) {
            overallStatus = "PARTIAL";
        } else if ("SUCCESS".equalsIgnoreCase(entry.getArchiveStatus()) && statuses.size() == 1) {
            overallStatus = "SUCCESS";
        } else {
            overallStatus = isErrorPresent ? "FAILED" : "FAILED"; // fallback if nothing else matched
        }

        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}
