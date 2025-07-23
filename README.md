private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.computeIfAbsent(key, k -> {
            ProcessedFileEntry newEntry = new ProcessedFileEntry();
            newEntry.setCustomerId(file.getCustomerId());
            newEntry.setAccountNumber(file.getAccountNumber());
            return newEntry;
        });

        String outputType = file.getOutputType();
        String blobUrl = file.getBlobUrl();
        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

        String status;
        if (isNonEmpty(blobUrl)) {
            status = "SUCCESS";
        } else if ("FAILED".equalsIgnoreCase(errors.getOrDefault(outputType, ""))) {
            status = "FAILED";
        } else {
            status = "NOT_FOUND";
        }

        switch (outputType) {
            case "EMAIL" -> {
                entry.setEmailBlobUrl(blobUrl);
                entry.setEmailStatus(status);
            }
            case "PRINT" -> {
                entry.setPrintFileUrl(blobUrl);
                entry.setPrintStatus(status);
            }
            case "MOBSTAT" -> {
                entry.setMobstatBlobUrl(blobUrl);
                entry.setMobstatStatus(status);
            }
            case "ARCHIVE" -> {
                entry.setArchiveBlobUrl(blobUrl);
                entry.setArchiveStatus(status);
            }
        }
    }

    // ✅ Enhanced overallStatus logic with combo handling
    for (ProcessedFileEntry entry : grouped.values()) {
        String email = entry.getEmailStatus();
        String print = entry.getPrintStatus();
        String mobstat = entry.getMobstatStatus();
        String archive = entry.getArchiveStatus();

        boolean isEmailSuccess = "SUCCESS".equals(email);
        boolean isPrintSuccess = "SUCCESS".equals(print);
        boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
        boolean isArchiveSuccess = "SUCCESS".equals(archive);

        boolean isEmailFailedOrMissing = email == null || "FAILED".equals(email) || "NOT_FOUND".equals(email);
        boolean isPrintFailedOrMissing = print == null || "FAILED".equals(print) || "NOT_FOUND".equals(print);
        boolean isMobstatFailedOrMissing = mobstat == null || "FAILED".equals(mobstat) || "NOT_FOUND".equals(mobstat);

        if (isEmailSuccess && isArchiveSuccess) {
            entry.setOverallStatus("SUCCESS");
        } else if (isMobstatSuccess && isArchiveSuccess && email == null && print == null) {
            entry.setOverallStatus("SUCCESS");
        } else if (isPrintSuccess && isArchiveSuccess && email == null && mobstat == null) {
            entry.setOverallStatus("SUCCESS");
        } else if (isArchiveSuccess && isEmailFailedOrMissing && mobstat == null && print == null) {
            entry.setOverallStatus("PARTIAL");
        } else if (isArchiveSuccess) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(grouped.values());
}

=========

// Collect all overallStatuses for individual records
Set<String> statuses = entries.stream()
    .map(ProcessedFileEntry::getOverallStatus)
    .collect(Collectors.toSet());

// Final overall status for the payload
String overallStatus;
if (statuses.size() == 1) {
    overallStatus = statuses.iterator().next();
} else if (statuses.contains("SUCCESS") && statuses.contains("FAILED")) {
    overallStatus = "PARTIAL";
} else if (statuses.contains("PARTIAL") || statuses.size() > 1) {
    overallStatus = "PARTIAL";
} else {
    overallStatus = "FAILED";
}
