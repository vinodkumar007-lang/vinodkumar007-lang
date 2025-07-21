private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
        .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
        .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        ProcessedFileEntry result = new ProcessedFileEntry();
        result.setCustomerId(customerId);
        result.setAccountNumber(accountNumber);

        boolean hasFailed = false;
        boolean hasNotFound = false;

        for (SummaryProcessedFile file : entry.getValue()) {
            String status = file.getStatus();
            String blobUrl = file.getBlobUrl();
            String method = file.getOutputType();

            if ("FAILED".equalsIgnoreCase(status)) hasFailed = true;
            if ("NOT-FOUND".equalsIgnoreCase(status)) hasNotFound = true;

            switch (method.toUpperCase()) {
                case "EMAIL":
                    result.setEmailUrl(blobUrl);
                    result.setEmailStatus(status);
                    break;
                case "ARCHIVE":
                    result.setArchiveUrl(blobUrl);
                    result.setArchiveStatus(status);
                    break;
                case "PRINT":
                    result.setPrintUrl(blobUrl);
                    result.setPrintStatus(status);
                    break;
                case "MOBSTAT":
                    result.setMobstatUrl(blobUrl);
                    result.setMobstatStatus(status);
                    break;
            }
        }

        // Set overall status
        if (hasFailed) {
            result.setOverallStatus("FAILED");
        } else if (hasNotFound) {
            result.setOverallStatus("PARTIAL");
        } else {
            result.setOverallStatus("SUCCESS");
        }

        finalList.add(result);
    }

    return finalList;
}
