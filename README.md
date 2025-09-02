Function is difficult to follow

private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> ignoredPrintFiles) {

        List<ProcessedFileEntry> allEntries = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>(); // customerId + accountNumber + archiveFilename

        // --- Input checks ---
        if (processedFiles == null || processedFiles.isEmpty()) {
            logger.warn("[buildProcessedFileEntries] processedFiles is null/empty. Returning empty list.");
            if (ignoredPrintFiles != null && !ignoredPrintFiles.isEmpty()) {
                logger.debug("[buildProcessedFileEntries] ignoredPrintFiles present but will not be used. count={}", ignoredPrintFiles.size());
            }
            return allEntries;
        }
        if (errorMap == null) {
            logger.warn("[buildProcessedFileEntries] errorMap is null. Using empty map to avoid NPEs.");
            errorMap = java.util.Collections.emptyMap();
        }
        if (ignoredPrintFiles == null) {
            logger.debug("[buildProcessedFileEntries] ignoredPrintFiles is null.");
        } else if (!ignoredPrintFiles.isEmpty()) {
            logger.debug("[buildProcessedFileEntries] ignoredPrintFiles provided (not used in this method). count={}", ignoredPrintFiles.size());
        }

        logger.info("[buildProcessedFileEntries] Start building entries. processedFilesCount={}", processedFiles.size());

        for (SummaryProcessedFile file : processedFiles) {
            if (file == null) {
                logger.debug("[buildProcessedFileEntries] Skipping null SummaryProcessedFile.");
                continue;
            }

            String archiveFileName = (file.getArchiveBlobUrl() != null)
                    ? new java.io.File(file.getArchiveBlobUrl()).getName()
                    : "";

            // Keep key construction semantics exactly as before (including possible "null" strings)
            String key = file.getCustomerId() + "|" + file.getAccountNumber() + "|" + archiveFileName;

            if (uniqueKeys.contains(key)) {
                logger.debug("[GT] Duplicate skipped. customerId={}, account={}, archiveFile={}",
                        file.getCustomerId(), file.getAccountNumber(), archiveFileName);
                continue;
            }
            uniqueKeys.add(key);

            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());
            entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
            entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
            entry.setPrintBlobUrl(file.getPrintFileUrl());
            entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

            // --- Ensure we get the correct account number from filename if needed (same logic) ---
            String account = file.getAccountNumber();
            if ((account == null || account.isBlank()) && isNonEmpty(file.getArchiveBlobUrl())) {
                account = extractAccountFromFileName(archiveFileName);
                logger.debug("[GT] Account missing. Extracted from archive file. customerId={}, extractedAccount={}, archiveFile={}",
                        file.getCustomerId(), account, archiveFileName);
            }

            Map<String, String> errors = errorMap.getOrDefault(account, java.util.Collections.emptyMap());

            // --- Set individual statuses (same conditions as original) ---
            entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
            entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
            entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
            entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

            // --- Determine overall status (unchanged logic) ---
            boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
            boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
            boolean printSuccess  = "SUCCESS".equals(entry.getPrintStatus());
            boolean archiveSuccess= "SUCCESS".equals(entry.getArchiveStatus());

            if ((emailSuccess && archiveSuccess) ||
                    (mobstatSuccess && archiveSuccess && !emailSuccess && !printSuccess) ||
                    (printSuccess && archiveSuccess && !emailSuccess && !mobstatSuccess)) {
                entry.setOverallStatus("SUCCESS");
            } else if (archiveSuccess) {
                entry.setOverallStatus("PARTIAL");
            } else {
                entry.setOverallStatus("FAILED");
            }

            // --- If any errors exist for this account, mark as PARTIAL if not FAILED (same logic) ---
            if (errorMap.containsKey(account) && !"FAILED".equals(entry.getOverallStatus())) {
                entry.setOverallStatus("PARTIAL");
            }

            logger.info("[GT] customerId={}, account={}, archiveFile={} | email={}, mobstat={}, print={}, archive={}, overall={}",
                    entry.getCustomerId(), account, archiveFileName,
                    entry.getEmailStatus(), entry.getMobstatStatus(), entry.getPrintStatus(),
                    entry.getArchiveStatus(), entry.getOverallStatus());

            allEntries.add(entry);
        }

        logger.info("[buildProcessedFileEntries] Completed. totalEntries={}", allEntries.size());
        return allEntries;
    }

    public static String extractAccountFromFileName(String fileName) {
        if (fileName == null || !fileName.contains("_")) return null;
        return fileName.split("_")[0];
    }
