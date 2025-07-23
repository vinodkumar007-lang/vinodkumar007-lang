private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles) {
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
                default:
                    logger.warn("⚠️ Unknown output method: {}", outputMethod);
                    break;
            }
        } else {
            logger.warn("⚠️ Null output method for customerId: {}, accountNumber: {}", file.getCustomerId(), file.getAccountNumber());
            // status fields remain unset ("") to allow PARTIAL evaluation
        }

        grouped.put(key, entry);
    }

    // Now evaluate overallStatus per entry
    for (ProcessedFileEntry entry : grouped.values()) {
        boolean hasFailed = false;
        boolean hasMissing = false;
        boolean allSuccess = true;

        List<String> statuses = Arrays.asList(
            entry.getEmailStatus(),
            entry.getPrintStatus(),
            entry.getMobstatStatus(),
            entry.getArchiveStatus()
        );

        for (String st : statuses) {
            if ("FAILED".equalsIgnoreCase(st)) {
                hasFailed = true;
                allSuccess = false;
            } else if (st == null || st.isEmpty()) {
                hasMissing = true;
                allSuccess = false;
            }
        }

        if (allSuccess) {
            entry.setOverallStatus("SUCCESS");
        } else if (hasFailed) {
            entry.setOverallStatus("FAILED");
        } else if (hasMissing) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("PARTIAL"); // fallback
        }
    }

    return new ArrayList<>(grouped.values());
}
