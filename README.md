private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<CustomerAccountEntry> customerAccounts,
        Map<String, Path> foundFilesMap,
        Map<String, String> errorMap,
        String archiveFolderBlobUrl
) {
    List<ProcessedFileEntry> processedFileEntries = new ArrayList<>();
    List<String> outputTypes = Arrays.asList("EMAIL", "MOBSTAT", "PRINT");

    for (CustomerAccountEntry entry : customerAccounts) {
        String customerId = entry.getCustomerId();
        String accountNumber = entry.getAccountNumber();
        String baseKey = customerId + "_" + accountNumber;

        for (String outputType : outputTypes) {
            String fileKey = baseKey + "_" + outputType;

            // === Create new entry per outputType ===
            ProcessedFileEntry processedEntry = new ProcessedFileEntry();
            processedEntry.setCustomerId(customerId);
            processedEntry.setAccountNumber(accountNumber);
            processedEntry.setOutputType(outputType);

            // === Set OutputType blobUrl and status ===
            if (foundFilesMap.containsKey(fileKey)) {
                String blobUrl = foundFilesMap.get(fileKey).toString(); // Replace with your Blob URL logic if needed
                processedEntry.setBlobUrl(blobUrl);
                processedEntry.setStatus("SUCCESS");
            } else {
                String errMethod = errorMap.get(fileKey);
                if (errMethod != null && errMethod.equalsIgnoreCase(outputType)) {
                    processedEntry.setStatus("FAILED");
                } else {
                    processedEntry.setStatus("NOT-FOUND");
                }
                processedEntry.setBlobUrl("");
            }

            // === Always add ARCHIVE ===
            processedEntry.setArchiveOutputType("ARCHIVE");
            processedEntry.setArchiveBlobUrl(archiveFolderBlobUrl + "/" + baseKey + ".pdf");
            processedEntry.setArchiveStatus("SUCCESS");

            // === Determine overallStatus ===
            if ("SUCCESS".equals(processedEntry.getStatus())) {
                processedEntry.setOverallStatus("SUCCESS");
            } else if ("FAILED".equals(processedEntry.getStatus())) {
                processedEntry.setOverallStatus("FAILED");
            } else {
                processedEntry.setOverallStatus("PARTIAL");
            }

            processedFileEntries.add(processedEntry);
        }
    }

    return processedFileEntries;
}

