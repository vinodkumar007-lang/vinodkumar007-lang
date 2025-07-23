private static List<ProcessedFileEntry> buildDetailedProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String method = file.getOutputMethod();
        String blobUrl = file.getBlobUrl();

        if ("ARCHIVE".equalsIgnoreCase(method)) {
            entry.setArchiveBlobUrl(blobUrl);
            entry.setArchiveStatus((blobUrl != null && !blobUrl.isEmpty()) ? "SUCCESS" : "");
        } else if ("EMAIL".equalsIgnoreCase(method)) {
            entry.setEmailBlobUrl(blobUrl);
            entry.setEmailStatus((blobUrl != null && !blobUrl.isEmpty()) ? "SUCCESS" : "");
        } else if ("PRINT".equalsIgnoreCase(method)) {
            entry.setPrintBlobUrl(blobUrl);
            entry.setPrintStatus((blobUrl != null && !blobUrl.isEmpty()) ? "SUCCESS" : "");
        } else if ("MOBSTAT".equalsIgnoreCase(method)) {
            entry.setMobstatBlobUrl(blobUrl);
            entry.setMobstatStatus((blobUrl != null && !blobUrl.isEmpty()) ? "SUCCESS" : "");
        }

        grouped.put(key, entry);
    }

    for (ProcessedFileEntry entry : grouped.values()) {
        String key = entry.getCustomerId() + "-" + entry.getAccountNumber();
        Map<String, String> errorMethods = errorMap.getOrDefault(key, new HashMap<>());

        // EMAIL
        if (entry.getEmailBlobUrl() == null || entry.getEmailBlobUrl().isEmpty()) {
            if ("EMAIL".equalsIgnoreCase(errorMethods.get("method"))) {
                entry.setEmailStatus("FAILED");
            } else if (errorMethods.isEmpty()) {
                entry.setEmailStatus("FAILED");
            } else {
                entry.setEmailStatus("NOT-FOUND");
            }
            entry.setEmailBlobUrl("");
        }

        // PRINT
        if (entry.getPrintBlobUrl() == null || entry.getPrintBlobUrl().isEmpty()) {
            if ("PRINT".equalsIgnoreCase(errorMethods.get("method"))) {
                entry.setPrintStatus("FAILED");
            } else if (errorMethods.isEmpty()) {
                entry.setPrintStatus("FAILED");
            } else {
                entry.setPrintStatus("NOT-FOUND");
            }
            entry.setPrintBlobUrl("");
        }

        // MOBSTAT
        if (entry.getMobstatBlobUrl() == null || entry.getMobstatBlobUrl().isEmpty()) {
            if ("MOBSTAT".equalsIgnoreCase(errorMethods.get("method"))) {
                entry.setMobstatStatus("FAILED");
            } else if (errorMethods.isEmpty()) {
                entry.setMobstatStatus("FAILED");
            } else {
                entry.setMobstatStatus("NOT-FOUND");
            }
            entry.setMobstatBlobUrl("");
        }

        // ARCHIVE must always be SUCCESS (based on assumption)
        if (entry.getArchiveBlobUrl() == null || entry.getArchiveBlobUrl().isEmpty()) {
            entry.setArchiveBlobUrl("");
            entry.setArchiveStatus("FAILED"); // safety fallback if missing
        }

        // Now determine overall status
        entry.setOverallStatus(determineOverallStatus(
                entry.getEmailStatus(),
                entry.getPrintStatus(),
                entry.getMobstatStatus(),
                entry.getArchiveStatus()
        ));
    }

    return new ArrayList<>(grouped.values());
}

===========

private static String determineOverallStatus(String emailStatus, String printStatus, String mobstatStatus, String archiveStatus) {
    if (!"SUCCESS".equalsIgnoreCase(archiveStatus)) {
        return "FAILED"; // archive is mandatory
    }

    List<String> statuses = Arrays.asList(emailStatus, printStatus, mobstatStatus);
    boolean allSuccessOrBlank = statuses.stream().allMatch(s -> s.equalsIgnoreCase("SUCCESS") || s.isEmpty());
    boolean anyFailed = statuses.stream().anyMatch(s -> s.equalsIgnoreCase("FAILED"));
    boolean anyPartial = statuses.stream().anyMatch(s -> s.equalsIgnoreCase("FAILED") || s.equalsIgnoreCase("NOT-FOUND"));

    if (allSuccessOrBlank) {
        return "SUCCESS";
    } else if (anyFailed || anyPartial) {
        return "PARTIAL";
    }
    return "FAILED";
}
