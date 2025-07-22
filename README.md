private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputType = file.getOutputType().toUpperCase();
        String blobUrl = file.getBlobUrl();

        switch (outputType) {
            case "EMAIL":
                entry.setEmailBlobUrl(blobUrl != null ? blobUrl : "");
                entry.setEmailStatus((blobUrl != null && !blobUrl.isEmpty()) ? "SUCCESS" : "");
                break;
            case "PRINT":
                entry.setPrintBlobUrl(blobUrl != null ? blobUrl : "");
                entry.setPrintStatus((blobUrl != null && !blobUrl.isEmpty()) ? "SUCCESS" : "");
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(blobUrl != null ? blobUrl : "");
                entry.setMobstatStatus((blobUrl != null && !blobUrl.isEmpty()) ? "SUCCESS" : "");
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(blobUrl != null ? blobUrl : "");
                entry.setArchiveStatus((blobUrl != null && !blobUrl.isEmpty()) ? "SUCCESS" : "FAILED");
                break;
        }

        grouped.put(key, entry);
    }

    // Check errors and update statuses
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

        // Archive is mandatory; already handled above with SUCCESS or FAILED
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

private static Map<String, Map<String, String>> buildErrorMap(List<String> errorLines) {
    Map<String, Map<String, String>> errorMap = new HashMap<>();

    for (String line : errorLines) {
        String[] parts = line.split("\\|");
        if (parts.length < 4) continue;

        String account = parts[0].replaceAll("_ARC", "");
        String customer = parts[1];
        String type = parts[3].toUpperCase();
        String status = parts.length > 4 ? parts[4].toUpperCase() : "FAILED";

        String key = customer + "-" + account;
        errorMap.computeIfAbsent(key, k -> new HashMap<>()).put(type, status);
    }

    return errorMap;
}
