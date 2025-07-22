private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles, Map<String, String> errorMap) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "::" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        List<OutputDetail> outputDetails = entry.getOutputs() != null ? entry.getOutputs() : new ArrayList<>();

        String outputType = file.getOutputType();
        String blobUrl = file.getBlobUrl();
        String status;
        boolean shouldAdd = true;

        if ("ARCHIVE".equalsIgnoreCase(outputType)) {
            // ARCHIVE always present and always SUCCESS
            status = "SUCCESS";
        } else {
            if (blobUrl != null && !blobUrl.isEmpty()) {
                status = "SUCCESS";
            } else {
                String errorKey = file.getCustomerId() + "::" + file.getAccountNumber();
                if (errorMap.containsKey(errorKey)) {
                    status = "FAILED";
                } else {
                    shouldAdd = false; // Skip adding this output type
                }
            }
        }

        if (shouldAdd) {
            OutputDetail detail = new OutputDetail();
            detail.setOutputType(outputType);
            detail.setBlobUrl(blobUrl);
            detail.setStatus(status);
            outputDetails.add(detail);
        }

        entry.setOutputs(outputDetails);
        // Calculate overallStatus
        String overallStatus = calculateOverallStatus(outputDetails);
        entry.setOverallStatus(overallStatus);

        grouped.put(key, entry);
    }

    return new ArrayList<>(grouped.values());
}

private static String calculateOverallStatus(List<OutputDetail> outputs) {
    long total = outputs.size();
    long success = outputs.stream().filter(o -> "SUCCESS".equalsIgnoreCase(o.getStatus())).count();
    long failed = outputs.stream().filter(o -> "FAILED".equalsIgnoreCase(o.getStatus())).count();

    if (success == total) return "SUCCESS";
    if (failed == total) return "FAILED";
    return "PARTIAL";
}
