public static List<SummaryProcessedFile> buildDetailedProcessedFiles(List<GeneratedFile> generatedFiles,
                                                                      List<ErrorReportEntry> errorReportEntries) {
    Map<String, SummaryProcessedFile> customerMap = new LinkedHashMap<>();

    Set<String> failedAccounts = errorReportEntries.stream()
            .map(ErrorReportEntry::getAccountNumber)
            .collect(Collectors.toSet());

    for (GeneratedFile file : generatedFiles) {
        String accountNumber = file.getAccountNumber();
        if (accountNumber == null || accountNumber.isBlank()) continue;

        // ✅ Skip if already added (OpenText includes same customer in multiple queues)
        if (customerMap.containsKey(accountNumber)) continue;

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

        customerMap.put(accountNumber, summary);
    }

    return new ArrayList<>(customerMap.values());
}
