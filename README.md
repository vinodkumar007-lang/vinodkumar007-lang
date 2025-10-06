private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> ignoredPrintFiles) {

    List<ProcessedFileEntry> allEntries = new ArrayList<>();
    Set<String> uniqueKeys = new HashSet<>();

    for (SummaryProcessedFile file : processedFiles) {
        if (file == null) continue;

        // --- Determine effective account number first ---
        String account = file.getAccountNumber();
        if ((account == null || account.isBlank()) && isNonEmpty(file.getArchiveBlobUrl())) {
            account = extractAccountFromFileName(new File(file.getArchiveBlobUrl()).getName());
        }

        // --- Skip entries with empty account number ---
        if (account == null || account.isBlank()) continue;

        // --- Deduplication key using effective account number ---
        String key = file.getCustomerId() + "|" + account + "|" +
                     (file.getArchiveBlobUrl() != null ? new File(file.getArchiveBlobUrl()).getName() : "");
        if (uniqueKeys.contains(key)) continue;
        uniqueKeys.add(key);

        // --- Build ProcessedFileEntry ---
        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(account);

        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());

        // --- Email blob URLs ---
        entry.setEmailBlobUrlPdf(file.getEmailBlobUrlPdf());
        entry.setEmailBlobUrlHtml(file.getEmailBlobUrlHtml());
        entry.setEmailBlobUrlText(file.getEmailBlobUrlText());

        Map<String, String> errors = errorMap.getOrDefault(account, Collections.emptyMap());
        entry.setEmailStatus(
                isNonEmpty(file.getEmailBlobUrlPdf()) ||
                isNonEmpty(file.getEmailBlobUrlHtml()) ||
                isNonEmpty(file.getEmailBlobUrlText()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : ""
        );

        // --- Other statuses ---
        entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

        // --- Determine overall status ---
        boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
        boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
        boolean printSuccess = "SUCCESS".equals(entry.getPrintStatus());
        boolean archiveSuccess = "SUCCESS".equals(entry.getArchiveStatus());

        if ((emailSuccess && archiveSuccess) ||
            (mobstatSuccess && archiveSuccess && !emailSuccess && !printSuccess) ||
            (printSuccess && archiveSuccess && !emailSuccess && !mobstatSuccess)) {
            entry.setOverallStatus("SUCCESS");
        } else if (archiveSuccess) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }

        if (errorMap.containsKey(account) && !"FAILED".equals(entry.getOverallStatus())) {
            entry.setOverallStatus("PARTIAL");
        }

        allEntries.add(entry);
    }

    return allEntries;
}
