private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        String requestedMethod
) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String method = file.getOutputMethod();
        String blobUrl = file.getBlobUrl();
        String status = file.getStatus();

        switch (method.toUpperCase()) {
            case "EMAIL":
                entry.setEmailBlobUrl(blobUrl);
                entry.setEmailStatus(status);
                break;
            case "PRINT":
                entry.setPrintBlobUrl(blobUrl);
                entry.setPrintStatus(status);
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(blobUrl);
                entry.setMobstatStatus(status);
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(blobUrl);
                entry.setArchiveStatus(status);
                break;
        }

        grouped.put(key, entry);
    }

    // After collecting from processedFiles, handle missing method or archive using errorMap
    for (ProcessedFileEntry entry : grouped.values()) {
        String key = entry.getCustomerId() + "-" + entry.getAccountNumber();
        Map<String, String> errorEntry = errorMap.getOrDefault(key, Collections.emptyMap());

        // Handle missing requested method
        if ("EMAIL".equalsIgnoreCase(requestedMethod) && isBlank(entry.getEmailStatus())) {
            if (errorEntry.containsKey("EMAIL")) {
                entry.setEmailStatus("FAILED");
            }
        }
        if ("PRINT".equalsIgnoreCase(requestedMethod) && isBlank(entry.getPrintStatus())) {
            if (errorEntry.containsKey("PRINT")) {
                entry.setPrintStatus("FAILED");
            }
        }
        if ("MOBSTAT".equalsIgnoreCase(requestedMethod) && isBlank(entry.getMobstatStatus())) {
            if (errorEntry.containsKey("MOBSTAT")) {
                entry.setMobstatStatus("FAILED");
            }
        }

        // ARCHIVE always expected, fallback to FAILED if present in error
        if (isBlank(entry.getArchiveStatus())) {
            if (errorEntry.containsKey("ARCHIVE")) {
                entry.setArchiveStatus("FAILED");
            }
        }

        // Now set overall status
        String overall = determineOverallStatus(entry, requestedMethod);
        entry.setOverallStatus(overall);
    }

    return new ArrayList<>(grouped.values());
}

private static String determineOverallStatus(ProcessedFileEntry entry, String requestedMethod) {
    String archiveStatus = normalize(entry.getArchiveStatus());
    String reqStatus = switch (requestedMethod.toUpperCase()) {
        case "EMAIL" -> normalize(entry.getEmailStatus());
        case "PRINT" -> normalize(entry.getPrintStatus());
        case "MOBSTAT" -> normalize(entry.getMobstatStatus());
        default -> "";
    };

    // Rule 1: both present and success
    if ("SUCCESS".equals(reqStatus) && "SUCCESS".equals(archiveStatus)) {
        return "SUCCESS";
    }

    // Rule 2: requested method FAILED and archive SUCCESS
    if ("FAILED".equals(reqStatus) && "SUCCESS".equals(archiveStatus)) {
        return "PARTIAL";
    }

    // Rule 3: requested method blank, archive success (but account found in errorMap)
    if (reqStatus.isEmpty() && "SUCCESS".equals(archiveStatus)) {
        return "PARTIAL";
    }

    // Final fallback
    return "FAILED";
}

private static String normalize(String status) {
    return status != null ? status.trim().toUpperCase() : "";
}

private static boolean isBlank(String str) {
    return str == null || str.trim().isEmpty();
}
