private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedList,
        Map<String, Map<String, String>> errorMap // key = customerId::accountNumber, value = Map<outputType, reason>
) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    // Group by customerId::accountNumber
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];
        List<SummaryProcessedFile> records = entry.getValue();

        // Map of outputType -> SummaryProcessedFile
        Map<String, SummaryProcessedFile> typeMap = records.stream()
                .collect(Collectors.toMap(SummaryProcessedFile::getOutputType, f -> f, (a, b) -> a));

        SummaryProcessedFile archive = typeMap.get("ARCHIVE");
        if (archive == null) continue; // Archive is mandatory

        // Collect all requested output types, including EMAIL/PRINT/MOBSTAT and any extras
        Set<String> requestedOutputs = new HashSet<>(errorMap.getOrDefault(entry.getKey(), Collections.emptyMap()).keySet());
        requestedOutputs.addAll(typeMap.keySet());
        requestedOutputs.remove("ARCHIVE"); // Exclude archive from outputs

        for (String outputMethod : requestedOutputs) {
            ProcessedFileEntry entryObj = new ProcessedFileEntry();
            entryObj.setCustomerId(customerId);
            entryObj.setAccountNumber(accountNumber);
            entryObj.setOutputMethod(outputMethod);

            // Archive info
            entryObj.setArchiveBlobUrl(archive.getBlobURL());
            entryObj.setArchiveStatus(archive.getStatus());

            SummaryProcessedFile output = typeMap.get(outputMethod);
            if (output != null) {
                entryObj.setOutputBlobUrl(output.getBlobURL());
                entryObj.setOutputStatus(output.getStatus());
            } else {
                String key = customerId + "::" + accountNumber;
                Map<String, String> failedMap = errorMap.getOrDefault(key, Collections.emptyMap());

                entryObj.setOutputBlobUrl(null);

                if (failedMap.containsKey(outputMethod)) {
                    entryObj.setOutputStatus("FAILED");
                } else {
                    entryObj.setOutputStatus("NOT-FOUND");
                }
            }

            // Overall Status computation
            String archiveStatus = entryObj.getArchiveStatus();
            String outputStatus = entryObj.getOutputStatus();

            if ("SUCCESS".equalsIgnoreCase(outputStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
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
