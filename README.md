private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> ignoredPrintFiles) {

        List<ProcessedFileEntry> allEntries = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>(); // customerId + accountNumber + archiveFilename

        for (SummaryProcessedFile file : processedFiles) {
            if (file == null) continue;

            String key = file.getCustomerId() + "|" + file.getAccountNumber() + "|" +
                    (file.getArchiveBlobUrl() != null ? new File(file.getArchiveBlobUrl()).getName() : "");
            if (uniqueKeys.contains(key)) continue;
            uniqueKeys.add(key);

            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());
            entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
            entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
            entry.setPrintBlobUrl(file.getPrintFileUrl());
            entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

            // --- Ensure we get the correct account number from filename if needed ---
            String account = file.getAccountNumber();
            if ((account == null || account.isBlank()) && isNonEmpty(file.getArchiveBlobUrl())) {
                account = extractAccountFromFileName(new File(file.getArchiveBlobUrl()).getName());
            }

            Map<String, String> errors = errorMap.getOrDefault(account, Collections.emptyMap());

            // --- Set individual statuses ---
            entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
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

            // --- If any errors exist for this account, mark as PARTIAL if not FAILED ---
            if (errorMap.containsKey(account) && !"FAILED".equals(entry.getOverallStatus())) {
                entry.setOverallStatus("PARTIAL");
            }

            allEntries.add(entry);
        }

        return allEntries;
    }

    // --- Helper to extract account from filename ---
    public static String extractAccountFromFileName(String fileName) {
        if (fileName == null || !fileName.contains("_")) return null;
        return fileName.split("_")[0];
    }
