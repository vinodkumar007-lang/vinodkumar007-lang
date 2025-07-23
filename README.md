private static List<DetailedProcessedFile> buildDetailedProcessedFiles(List<SummaryProcessedFile> processedFiles) {
    List<DetailedProcessedFile> detailedList = new ArrayList<>();

    for (SummaryProcessedFile file : processedFiles) {
        DetailedProcessedFile detail = new DetailedProcessedFile();
        detail.setCustomerId(file.getCustomerId());
        detail.setAccountNumber(file.getAccountNumber());
        detail.setOutputType(file.getOutputType());
        detail.setBlobUrl(defaultIfNull(file.getBlobUrl()));
        detail.setStatus(isNonEmpty(file.getBlobUrl()) ? "SUCCESS" : "FAILED");
        detailedList.add(detail);
    }

    return detailedList;
}
private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputType = file.getOutputType();
        String blobUrl = defaultIfNull(file.getBlobUrl());
        String status = defaultIfNull(file.getStatus());

        switch (outputType) {
            case "EMAIL":
                entry.setEmailUrl(blobUrl);
                entry.setEmailStatus(status);
                break;
            case "PRINT":
                entry.setPrintUrl(blobUrl);
                entry.setPrintStatus(status);
                break;
            case "MOBSTAT":
                entry.setMobstatUrl(blobUrl);
                entry.setMobstatStatus(status);
                break;
            case "ARCHIVE":
                entry.setArchiveUrl(blobUrl);
                entry.setArchiveStatus(status);
                break;
        }

        grouped.put(key, entry);
    }

    // Finalize overallStatus
    for (ProcessedFileEntry entry : grouped.values()) {
        String emailStatus = defaultIfNull(entry.getEmailStatus());
        String archiveStatus = defaultIfNull(entry.getArchiveStatus());

        if (emailStatus.equals("SUCCESS") && archiveStatus.equals("SUCCESS")) {
            entry.setOverallStatus("SUCCESS");
        } else if (emailStatus.equals("FAILED") && archiveStatus.equals("FAILED")) {
            entry.setOverallStatus("FAILED");
        } else if (emailStatus.equals("SUCCESS") || archiveStatus.equals("SUCCESS")) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }

        // Ensure non-null defaults for all statuses/URLs
        entry.setEmailUrl(defaultIfNull(entry.getEmailUrl()));
        entry.setPrintUrl(defaultIfNull(entry.getPrintUrl()));
        entry.setMobstatUrl(defaultIfNull(entry.getMobstatUrl()));
        entry.setArchiveUrl(defaultIfNull(entry.getArchiveUrl()));

        entry.setEmailStatus(defaultIfNull(entry.getEmailStatus()));
        entry.setPrintStatus(defaultIfNull(entry.getPrintStatus()));
        entry.setMobstatStatus(defaultIfNull(entry.getMobstatStatus()));
        entry.setArchiveStatus(defaultIfNull(entry.getArchiveStatus()));
    }

    return new ArrayList<>(grouped.values());
}

private static String defaultIfNull(String value) {
    return value == null ? "" : value;
}

private static boolean isNonEmpty(String value) {
    return value != null && !value.trim().isEmpty();
}
