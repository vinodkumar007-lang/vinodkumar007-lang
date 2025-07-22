public static List<ProcessedFileEntry> buildProcessedFileEntriesGrouped(
        List<SummaryProcessedFile> summaryFiles
) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : summaryFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();

        grouped.computeIfAbsent(key, k -> {
            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());
            entry.setDeliveryStatuses(new ArrayList<>());
            return entry;
        });

        DeliveryStatus status = new DeliveryStatus();
        status.setOutputType(file.getOutputType());
        status.setBlobUrl(file.getBlobUrl());
        status.setStatus(file.getStatus());

        grouped.get(key).getDeliveryStatuses().add(status);
    }

    // Now calculate overallStatus for each entry
    for (ProcessedFileEntry entry : grouped.values()) {
        boolean hasSuccess = false;
        boolean hasFailed = false;

        for (DeliveryStatus d : entry.getDeliveryStatuses()) {
            if ("SUCCESS".equalsIgnoreCase(d.getStatus())) {
                hasSuccess = true;
            } else if ("FAILED".equalsIgnoreCase(d.getStatus())) {
                hasFailed = true;
            }
        }

        String overall;
        if (hasSuccess && hasFailed) {
            overall = "PARTIAL";
        } else if (hasSuccess) {
            overall = "SUCCESS";
        } else {
            overall = "FAILED";
        }

        entry.setOverallStatus(overall);
    }

    return new ArrayList<>(grouped.values());
}
