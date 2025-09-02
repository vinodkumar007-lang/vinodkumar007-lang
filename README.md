private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> ignoredPrintFiles) {

    // --- Validate inputs ---
    processedFiles = validateProcessedFiles(processedFiles, ignoredPrintFiles);
    if (processedFiles.isEmpty()) return Collections.emptyList();
    if (errorMap == null) {
        logger.warn("[buildProcessedFileEntries] errorMap is null. Using empty map.");
        errorMap = Collections.emptyMap();
    }

    logger.info("[buildProcessedFileEntries] Start building entries. processedFilesCount={}", processedFiles.size());

    List<ProcessedFileEntry> allEntries = new ArrayList<>();
    Set<String> uniqueKeys = new HashSet<>();

    for (SummaryProcessedFile file : processedFiles) {
        if (file == null) {
            logger.debug("[buildProcessedFileEntries] Skipping null SummaryProcessedFile.");
            continue;
        }

        String archiveFileName = extractArchiveFileName(file);
        String key = generateUniqueKey(file, archiveFileName);

        if (!uniqueKeys.add(key)) {
            logger.debug("[GT] Duplicate skipped. customerId={}, account={}, archiveFile={}",
                    file.getCustomerId(), file.getAccountNumber(), archiveFileName);
            continue;
        }

        String account = getAccount(file, archiveFileName);

        Map<String, String> errors = errorMap.getOrDefault(account, Collections.emptyMap());

        ProcessedFileEntry entry = mapToProcessedFileEntry(file, errors);
        entry.setOverallStatus(determineOverallStatus(entry, account, errorMap));

        logger.info("[GT] customerId={}, account={}, archiveFile={} | email={}, mobstat={}, print={}, archive={}, overall={}",
                entry.getCustomerId(), account, archiveFileName,
                entry.getEmailStatus(), entry.getMobstatStatus(), entry.getPrintStatus(),
                entry.getArchiveStatus(), entry.getOverallStatus());

        allEntries.add(entry);
    }

    logger.info("[buildProcessedFileEntries] Completed. totalEntries={}", allEntries.size());
    return allEntries;
}

// --- Helper Methods ---

private static List<SummaryProcessedFile> validateProcessedFiles(List<SummaryProcessedFile> processedFiles, List<PrintFile> ignoredPrintFiles) {
    if (processedFiles == null || processedFiles.isEmpty()) {
        logger.warn("[buildProcessedFileEntries] processedFiles is null/empty. Returning empty list.");
        if (ignoredPrintFiles != null && !ignoredPrintFiles.isEmpty()) {
            logger.debug("[buildProcessedFileEntries] ignoredPrintFiles present but not used. count={}", ignoredPrintFiles.size());
        }
        return Collections.emptyList();
    }
    if (ignoredPrintFiles == null) {
        logger.debug("[buildProcessedFileEntries] ignoredPrintFiles is null.");
    } else if (!ignoredPrintFiles.isEmpty()) {
        logger.debug("[buildProcessedFileEntries] ignoredPrintFiles provided (not used). count={}", ignoredPrintFiles.size());
    }
    return processedFiles;
}

private static String extractArchiveFileName(SummaryProcessedFile file) {
    return file.getArchiveBlobUrl() != null ? new java.io.File(file.getArchiveBlobUrl()).getName() : "";
}

private static String generateUniqueKey(SummaryProcessedFile file, String archiveFileName) {
    return file.getCustomerId() + "|" + file.getAccountNumber() + "|" + archiveFileName;
}

private static String getAccount(SummaryProcessedFile file, String archiveFileName) {
    String account = file.getAccountNumber();
    if ((account == null || account.isBlank()) && isNonEmpty(file.getArchiveBlobUrl())) {
        account = extractAccountFromFileName(archiveFileName);
        logger.debug("[GT] Account missing. Extracted from archive file. customerId={}, extractedAccount={}, archiveFile={}",
                file.getCustomerId(), account, archiveFileName);
    }
    return account;
}

private static ProcessedFileEntry mapToProcessedFileEntry(SummaryProcessedFile file, Map<String, String> errors) {
    ProcessedFileEntry entry = new ProcessedFileEntry();
    entry.setCustomerId(file.getCustomerId());
    entry.setAccountNumber(file.getAccountNumber());
    entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
    entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
    entry.setPrintBlobUrl(file.getPrintFileUrl());
    entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

    entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
    entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
    entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
    entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

    return entry;
}

private static String determineOverallStatus(ProcessedFileEntry entry, String account, Map<String, Map<String, String>> errorMap) {
    boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
    boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
    boolean printSuccess  = "SUCCESS".equals(entry.getPrintStatus());
    boolean archiveSuccess= "SUCCESS".equals(entry.getArchiveStatus());

    String overallStatus;
    if ((emailSuccess && archiveSuccess) ||
        (mobstatSuccess && archiveSuccess && !emailSuccess && !printSuccess) ||
        (printSuccess && archiveSuccess && !emailSuccess && !mobstatSuccess)) {
        overallStatus = "SUCCESS";
    } else if (archiveSuccess) {
        overallStatus = "PARTIAL";
    } else {
        overallStatus = "FAILED";
    }

    if (errorMap.containsKey(account) && !"FAILED".equals(overallStatus)) {
        overallStatus = "PARTIAL";
    }

    return overallStatus;
}

public static String extractAccountFromFileName(String fileName) {
    if (fileName == null || !fileName.contains("_")) return null;
    return fileName.split("_")[0];
}
