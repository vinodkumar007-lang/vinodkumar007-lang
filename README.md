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
        String status = file.getStatus();

        if ("EMAIL".equalsIgnoreCase(outputType)) {
            entry.setEmailURL(blobUrl);
            entry.setEmailStatus(status);
        } else if ("ARCHIVE".equalsIgnoreCase(outputType)) {
            entry.setArchiveURL(blobUrl);
            entry.setArchiveStatus(status);
        } else if ("PRINT".equalsIgnoreCase(outputType)) {
            entry.setPrintURL(blobUrl);
            entry.setPrintStatus(status);
        } else if ("MOBSTAT".equalsIgnoreCase(outputType)) {
            entry.setMobstatURL(blobUrl);
            entry.setMobstatStatus(status);
        }

        grouped.put(key, entry);
    }

    for (ProcessedFileEntry entry : grouped.values()) {
        Map<String, String> customerErrors = errorMap.getOrDefault(
            entry.getCustomerId() + "-" + entry.getAccountNumber(), Collections.emptyMap());

        entry.setEmailStatus(setFinalStatus(entry.getEmailStatus(), customerErrors.get("EMAIL")));
        entry.setArchiveStatus(setFinalStatus(entry.getArchiveStatus(), customerErrors.get("ARCHIVE")));
        entry.setPrintStatus(setFinalStatus(entry.getPrintStatus(), customerErrors.get("PRINT")));
        entry.setMobstatStatus(setFinalStatus(entry.getMobstatStatus(), customerErrors.get("MOBSTAT")));

        // ✅ Improved logic for overallStatus
        Set<String> deliveryStatuses = new HashSet<>();
        if (entry.getEmailStatus() != null) deliveryStatuses.add(entry.getEmailStatus());
        if (entry.getPrintStatus() != null) deliveryStatuses.add(entry.getPrintStatus());
        if (entry.getMobstatStatus() != null) deliveryStatuses.add(entry.getMobstatStatus());

        String archive = entry.getArchiveStatus();

        if (deliveryStatuses.stream().allMatch("SUCCESS"::equals) && "SUCCESS".equals(archive)) {
            entry.setOverallStatus("SUCCESS");
        } else if (deliveryStatuses.stream().allMatch("FAILED"::equals) && "FAILED".equals(archive)) {
            entry.setOverallStatus("FAILED");
        } else if ("SUCCESS".equals(archive) && deliveryStatuses.contains("SUCCESS")) {
            entry.setOverallStatus("SUCCESS");
        } else if ("SUCCESS".equals(archive)) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(grouped.values());
}

private static String setFinalStatus(String originalStatus, String errorStatus) {
    if (originalStatus != null) {
        return originalStatus;
    } else if ("Failed".equalsIgnoreCase(errorStatus)) {
        return "FAILED";
    } else {
        return "NOT_FOUND";
    }
}

