private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    // Group by customerId + accountNumber
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
        .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
        .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        // Separate by output types
        Map<String, SummaryProcessedFile> typeMap = entry.getValue().stream()
            .collect(Collectors.toMap(SummaryProcessedFile::getOutputType, f -> f, (a, b) -> a));

        // Loop over EMAIL, MOBSTAT, PRINT
        for (String type : List.of("EMAIL", "MOBSTAT", "PRINT")) {
            SummaryProcessedFile main = typeMap.get(type);
            SummaryProcessedFile archive = typeMap.get("ARCHIVE");

            if (archive == null) continue; // Skip if archive missing, as per your rule

            ProcessedFileEntry entryObj = new ProcessedFileEntry();
            entryObj.setCustomerId(customerId);
            entryObj.setAccountNumber(accountNumber);
            entryObj.setOutputType(type);

            // Set archive fields (always mandatory)
            entryObj.setArchiveBlobUrl(archive.getBlobUrl());
            entryObj.setArchiveStatus(archive.getStatus());

            // Set outputType specific fields
            if (main != null) {
                entryObj.setOutputBlobUrl(main.getBlobUrl());
                entryObj.setOutputStatus(main.getStatus());
            } else {
                // Not found scenario: no record in file
                entryObj.setOutputBlobUrl(null);
                entryObj.setOutputStatus("NOT-FOUND");
            }

            // Determine overallStatus
            String outputStatus = entryObj.getOutputStatus();
            if ("SUCCESS".equalsIgnoreCase(outputStatus)) {
                entryObj.setOverallStatus("SUCCESS");
            } else if ("FAILED".equalsIgnoreCase(outputStatus)) {
                entryObj.setOverallStatus("FAILED");
            } else {
                entryObj.setOverallStatus("PARTIAL");
            }

            finalList.add(entryObj);
        }
    }

    return finalList;
}
