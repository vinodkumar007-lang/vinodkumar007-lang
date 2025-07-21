private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedList,
        Map<String, String> errorMap,
        Map<String, String> archiveUrlMap) {

    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        // Archive must be present — skip if missing
        String archiveKey = customerId + "|" + accountNumber;
        String archiveUrl = archiveUrlMap.get(archiveKey);
        if (archiveUrl == null) continue;

        List<SummaryProcessedFile> customerFiles = entry.getValue();

        for (String method : Arrays.asList("EMAIL", "MOBSTAT", "PRINT")) {
            ProcessedFileEntry fileEntry = new ProcessedFileEntry();
            fileEntry.setCustomerId(customerId);
            fileEntry.setAccountNumber(accountNumber);
            fileEntry.setOutputMethod(method);

            Optional<SummaryProcessedFile> match = customerFiles.stream()
                    .filter(f -> method.equalsIgnoreCase(f.getOutputType()))
                    .findFirst();

            String errorKey = customerId + "|" + accountNumber + "|" + method;
            boolean hasError = errorMap.containsKey(errorKey);

            if (match.isPresent()) {
                SummaryProcessedFile matched = match.get();
                fileEntry.setOutputBlobUrl(matched.getBlobUrl());

                if (hasError || matched.getBlobUrl() == null) {
                    fileEntry.setOutputStatus("FAILED");
                } else {
                    fileEntry.setOutputStatus("SUCCESS");
                }
            } else {
                fileEntry.setOutputBlobUrl(null);
                fileEntry.setOutputStatus("NOT-FOUND");
            }

            // Archive always present at this point
            fileEntry.setArchiveBlobUrl(archiveUrl);
            fileEntry.setArchiveStatus("SUCCESS");

            // Overall status logic
            if ("SUCCESS".equals(fileEntry.getOutputStatus()) && "SUCCESS".equals(fileEntry.getArchiveStatus())) {
                fileEntry.setOverallStatus("SUCCESS");
            } else if (!"FAILED".equals(fileEntry.getOutputStatus())) {
                fileEntry.setOverallStatus("PARTIAL");
            } else {
                fileEntry.setOverallStatus("FAILED");
            }

            finalList.add(fileEntry);
        }
    }

    return finalList;
}
