private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber() + "-" + file.getOutputType(); // 🔧 fix: include outputType
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

        // 🔧 Compute overallStatus based on output type
        String archiveStatus = "SUCCESS".equals(entry.getArchiveStatus()) ? "SUCCESS" : null;
        String deliveryStatus = switch (outputType) {
            case "EMAIL" -> entry.getEmailStatus();
            case "PRINT" -> entry.getPrintStatus();
            case "MOBSTAT" -> entry.getMobstatStatus();
            default -> null;
        };

        String overallStatus;
        if ("SUCCESS".equals(deliveryStatus) && "SUCCESS".equals(archiveStatus)) {
            overallStatus = "SUCCESS";
        } else if ("FAILED".equals(deliveryStatus) && "FAILED".equals(entry.getArchiveStatus())) {
            overallStatus = "FAILED";
        } else if ("SUCCESS".equals(archiveStatus) && 
                  ("FAILED".equals(deliveryStatus) || "NOT_FOUND".equals(deliveryStatus))) {
            overallStatus = "PARTIAL";
        } else if ("SUCCESS".equals(archiveStatus)) {
            overallStatus = "SUCCESS";
        } else {
            overallStatus = "FAILED";
        }

        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}
