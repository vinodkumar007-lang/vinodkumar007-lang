private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedList,
        Map<String, Set<String>> errorMap // key = customerId::accountNumber, value = Set of failed outputTypes
) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    // Group by customer + accountNumber
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];
        List<SummaryProcessedFile> records = entry.getValue();

        // Lookup map for quick access
        Map<String, SummaryProcessedFile> typeMap = records.stream()
                .collect(Collectors.toMap(SummaryProcessedFile::getOutputType, f -> f, (a, b) -> a));

        SummaryProcessedFile archive = typeMap.get("ARCHIVE");
        if (archive == null) continue; // skip if archive is missing

        for (String outputMethod : List.of("EMAIL", "PRINT", "MOBSTAT")) {
            SummaryProcessedFile output = typeMap.get(outputMethod);

            ProcessedFileEntry entryObj = new ProcessedFileEntry();
            entryObj.setCustomerId(customerId);
            entryObj.setAccountNumber(accountNumber);
            entryObj.setOutputMethod(outputMethod);

            // Archive values (always present)
            entryObj.setArchiveBlobUrl(archive.getBlobUrl());
            entryObj.setArchiveStatus(archive.getStatus());

            // Output method blob URL and status
            if (output != null) {
                // Found output file
                entryObj.setOutputBlobUrl(output.getBlobUrl());
                entryObj.setOutputStatus(output.getStatus());
            } else {
                // Check errorMap
                String key = customerId + "::" + accountNumber;
                Set<String> failedOutputs = errorMap.getOrDefault(key, Collections.emptySet());

                entryObj.setOutputBlobUrl(null);
                if (failedOutputs.contains(outputMethod)) {
                    entryObj.setOutputStatus("FAILED");
                } else {
                    entryObj.setOutputStatus("NOT-FOUND");
                }
            }

            // Determine overall status
            switch (entryObj.getOutputStatus()) {
                case "SUCCESS":
                    entryObj.setOverallStatus("SUCCESS");
                    break;
                case "FAILED":
                    entryObj.setOverallStatus("FAILED");
                    break;
                case "NOT-FOUND":
                default:
                    entryObj.setOverallStatus("PARTIAL");
                    break;
            }

            finalList.add(entryObj);
        }
    }

    return finalList;
}
