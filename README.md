private static List<SummaryProcessedFile> buildDetailedProcessedFiles(
        List<OutputFileInfo> outputFiles, Map<String, Map<String, String>> errorMap) {

    List<SummaryProcessedFile> summaryList = new ArrayList<>();

    for (OutputFileInfo file : outputFiles) {
        String customerId = file.getCustomerId();
        String accountNumber = file.getAccountNumber();

        // List of output types to check
        List<String> outputTypes = Arrays.asList("EMAIL", "PRINT", "MOBSTAT", "ARCHIVE");

        for (String outputType : outputTypes) {
            SummaryProcessedFile entry = new SummaryProcessedFile();
            entry.setCustomerId(customerId);
            entry.setAccountNumber(accountNumber);
            entry.setOutputType(outputType);

            // Look for matching output file for this type
            Optional<OutputFileInfo.OutputDetail> match = file.getOutputs().stream()
                    .filter(o -> outputType.equalsIgnoreCase(o.getOutputType()))
                    .findFirst();

            if (match.isPresent()) {
                entry.setBlobUrl(match.get().getBlobUrl());
                // Status and overallStatus will be filled later
            } else {
                entry.setBlobUrl(""); // empty for non-found, status decided later
            }

            summaryList.add(entry);
        }
    }

    return summaryList;
}
