private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles, Map<String, String> errorMap) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String customerId = file.getCustomerId();
        String accountNumber = file.getAccountNumber();
        String outputType = file.getOutputType(); // EMAIL, ARCHIVE, PRINT, MOBSTAT
        String blobUrl = file.getBlobUrl();

        String key = customerId + "::" + accountNumber;
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);

        OutputDetail detail = new OutputDetail();
        detail.setOutputType(outputType);
        detail.setBlobUrl(blobUrl);

        if (blobUrl != null && !blobUrl.isEmpty()) {
            detail.setStatus("SUCCESS");
        } else {
            if ("ARCHIVE".equalsIgnoreCase(outputType)) {
                detail.setStatus("FAILED"); // archive should always be present, but fallback
            } else if (errorMap.containsKey(key)) {
                detail.setStatus("FAILED");
            } else {
                // For PRINT, EMAIL, MOBSTAT → if not found and not in errorMap, skip
                continue;
            }
        }

        // Add this output type to the entry
        if (entry.getOutputs() == null) {
            entry.setOutputs(new ArrayList<>());
        }
        entry.getOutputs().add(detail);
        grouped.put(key, entry);
    }

    // Set overallStatus per entry
    for (ProcessedFileEntry entry : grouped.values()) {
        List<OutputDetail> outputs = entry.getOutputs();
        long successCount = outputs.stream()
            .filter(o -> "SUCCESS".equalsIgnoreCase(o.getStatus()))
            .count();
        long failedCount = outputs.stream()
            .filter(o -> "FAILED".equalsIgnoreCase(o.getStatus()))
            .count();

        String overallStatus;
        if (successCount > 0 && failedCount == 0) {
            overallStatus = "SUCCESS";
        } else if (successCount > 0 && failedCount > 0) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "FAILED";
        }

        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}
