public static List<SummaryProcessedFile> buildDetailedProcessedFiles(List<GeneratedFile> generatedFiles,
                                                                      List<ErrorReportEntry> errorReportEntries,
                                                                      Set<String> uniqueCustomerAccountsOut) {
    List<SummaryProcessedFile> processedFiles = new ArrayList<>();
    Set<String> uniqueAccounts = new HashSet<>();

    Set<String> failedAccounts = errorReportEntries.stream()
            .map(ErrorReportEntry::getAccountNumber)
            .collect(Collectors.toSet());

    for (GeneratedFile file : generatedFiles) {
        String accountNumber = file.getAccountNumber();
        if (accountNumber == null || accountNumber.isBlank()) continue;

        uniqueAccounts.add(accountNumber); // ✅ Count all customers

        SummaryProcessedFile summary = new SummaryProcessedFile();
        summary.setAccountNumber(accountNumber);
        summary.setCisNumber(file.getCisNumber());
        summary.setBlobFileUrl(file.getBlobUrl());
        summary.setFileName(file.getFileName());

        // ✅ Determine status
        String fileNameLower = file.getFileName().toLowerCase();
        if (fileNameLower.contains("error.pdf") || failedAccounts.contains(accountNumber)) {
            summary.setStatus("FAILED");
        } else {
            summary.setStatus("PROCESSED");
        }

        processedFiles.add(summary); // 👈 Don't deduplicate
    }

    // Pass back unique customer count for `customersProcessed`
    if (uniqueCustomerAccountsOut != null) {
        uniqueCustomerAccountsOut.addAll(uniqueAccounts);
    }

    return processedFiles;
}

Set<String> uniqueAccounts = new HashSet<>();

List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(
        generatedFiles,
        errorReportEntries,
        uniqueAccounts // 👈 this will now have all 24 customers
);

summaryPayload.getMetadata().setCustomersProcessed(uniqueAccounts.size());
