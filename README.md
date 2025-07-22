private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles,
                                                                   Map<String, String> errorMap) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        switch (file.getOutputType()) {
            case "EMAIL":
                if (isBlank(entry.getEmailBlobUrl())) {
                    entry.setEmailBlobUrl(file.getBlobUrl() != null ? file.getBlobUrl() : "");
                    entry.setEmailStatus(file.getStatus() != null ? file.getStatus() : "");
                }
                break;
            case "ARCHIVE":
                if (isBlank(entry.getArchiveBlobUrl())) {
                    entry.setArchiveBlobUrl(file.getBlobUrl() != null ? file.getBlobUrl() : "");
                    entry.setArchiveStatus(file.getStatus() != null ? file.getStatus() : "");
                }
                break;
            case "PRINT":
                if (isBlank(entry.getPrintBlobUrl())) {
                    entry.setPrintBlobUrl(file.getBlobUrl() != null ? file.getBlobUrl() : "");
                    entry.setPrintStatus(file.getStatus() != null ? file.getStatus() : "");
                }
                break;
            case "MOBSTAT":
                if (isBlank(entry.getMobstatBlobUrl())) {
                    entry.setMobstatBlobUrl(file.getBlobUrl() != null ? file.getBlobUrl() : "");
                    entry.setMobstatStatus(file.getStatus() != null ? file.getStatus() : "");
                }
                break;
        }

        grouped.put(key, entry);
    }

    // Inject FAILED records from errorMap if EMAIL/PRINT/MOBSTAT missing
    for (Map.Entry<String, String> errorEntry : errorMap.entrySet()) {
        String[] parts = errorEntry.getKey().split("-");
        if (parts.length != 3) continue;

        String customerId = parts[0];
        String accountNumber = parts[1];
        String outputType = parts[2];

        String key = customerId + "-" + accountNumber;
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);

        // Only set FAILED status if not already set
        switch (outputType) {
            case "EMAIL":
                if (isBlank(entry.getEmailBlobUrl()) && isBlank(entry.getEmailStatus())) {
                    entry.setEmailBlobUrl("");
                    entry.setEmailStatus("FAILED");
                }
                break;
            case "PRINT":
                if (isBlank(entry.getPrintBlobUrl()) && isBlank(entry.getPrintStatus())) {
                    entry.setPrintBlobUrl("");
                    entry.setPrintStatus("FAILED");
                }
                break;
            case "MOBSTAT":
                if (isBlank(entry.getMobstatBlobUrl()) && isBlank(entry.getMobstatStatus())) {
                    entry.setMobstatBlobUrl("");
                    entry.setMobstatStatus("FAILED");
                }
                break;
        }

        grouped.put(key, entry);
    }

    // Always ensure ARCHIVE has URL and status set (even if missing)
    for (ProcessedFileEntry entry : grouped.values()) {
        if (entry.getArchiveBlobUrl() == null) entry.setArchiveBlobUrl("");
        if (entry.getArchiveStatus() == null) entry.setArchiveStatus("");
        if (entry.getEmailBlobUrl() == null) entry.setEmailBlobUrl("");
        if (entry.getEmailStatus() == null) entry.setEmailStatus("");
        if (entry.getPrintBlobUrl() == null) entry.setPrintBlobUrl("");
        if (entry.getPrintStatus() == null) entry.setPrintStatus("");
        if (entry.getMobstatBlobUrl() == null) entry.setMobstatBlobUrl("");
        if (entry.getMobstatStatus() == null) entry.setMobstatStatus("");
    }

    return new ArrayList<>(grouped.values());
}
