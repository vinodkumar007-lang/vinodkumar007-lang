private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

    List<String> deliveryFolders = List.of(
            AppConstants.FOLDER_EMAIL,
            AppConstants.FOLDER_MOBSTAT,
            AppConstants.FOLDER_PRINT
    );

    Map<String, String> folderToOutputMethod = Map.of(
            AppConstants.FOLDER_EMAIL, AppConstants.OUTPUT_EMAIL,
            AppConstants.FOLDER_MOBSTAT, AppConstants.OUTPUT_MOBSTAT,
            AppConstants.FOLDER_PRINT, AppConstants.OUTPUT_PRINT
    );

    // -------- Upload all archive files and map by account + filename --------
    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    Map<String, Map<String, String>> accountToArchiveMap = new HashMap<>(); // account -> (filename -> URL)
    if (Files.exists(archivePath)) {
        try (Stream<Path> stream = Files.walk(archivePath)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                String fileName = file.getFileName().toString();
                boolean isMfcFile = fileName.startsWith("MFC"); // detect MFC files
                String account = extractAccountFromFileName(fileName, isMfcFile);
                if (account == null) return;

                try {
                    String archiveUrl = decodeUrl(
                            blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg)
                    );
                    accountToArchiveMap.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, archiveUrl);
                    logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveUrl);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                }
            });
        }
    }

    // -------- Upload delivery files and map by filename --------
    Map<String, String> emailFileMap = new HashMap<>();
    Map<String, String> mobstatFileMap = new HashMap<>();
    Map<String, String> printFileMap = new HashMap<>();

    for (String folder : deliveryFolders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                String fileName = file.getFileName().toString();
                boolean isMfcFile = fileName.startsWith("MFC"); // detect MFC files
                try {
                    String url = decodeUrl(
                            blobStorageService.uploadFileByMessage(file.toFile(), folder, msg)
                    );
                    switch (folder) {
                        case AppConstants.FOLDER_EMAIL -> emailFileMap.put(fileName, url);
                        case AppConstants.FOLDER_MOBSTAT -> mobstatFileMap.put(fileName, url);
                        case AppConstants.FOLDER_PRINT -> printFileMap.put(fileName, url);
                    }
                    logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folderToOutputMethod.get(folder), url);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(), folderToOutputMethod.get(folder), fileName, e.getMessage(), e);
                }
            });
        }
    }

    // -------- Build final list: only matching customer + account --------
    Set<String> uniqueKeys = new HashSet<>(); // customerId + accountNumber + archiveFilename

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;

        String account = customer.getAccountNumber();
        Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());

        for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
            String archiveFileName = archiveEntry.getKey();
            String archiveUrl = archiveEntry.getValue();

            // ✅ Prevent duplicates
            String key = customer.getCustomerId() + "|" + account + "|" + archiveFileName;
            if (uniqueKeys.contains(key)) continue;
            uniqueKeys.add(key);

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setArchiveBlobUrl(archiveUrl);

            // Match delivery files only if filename matches
            entry.setPdfEmailFileUrl(emailFileMap.get(archiveFileName));
            entry.setPdfMobstatFileUrl(mobstatFileMap.get(archiveFileName));
            entry.setPrintFileUrl(printFileMap.get(archiveFileName));

            finalList.add(entry);
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

// --- Updated helper for MFC ---
public static String extractAccountFromFileName(String fileName, boolean isMfcFile) {
    if (fileName == null || !fileName.contains("_")) return null;
    String[] parts = fileName.split("_");
    return isMfcFile ? parts[1] : parts[0]; // second part for MFC, first for normal
}

private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> ignoredPrintFiles) {

    List<ProcessedFileEntry> allEntries = new ArrayList<>();
    Set<String> uniqueKeys = new HashSet<>(); // customerId + accountNumber + archiveFilename

    for (SummaryProcessedFile file : processedFiles) {
        if (file == null) continue;

        boolean isMfcFile = file.getArchiveBlobUrl() != null && 
                            new File(file.getArchiveBlobUrl()).getName().startsWith("MFC");

        String accountFromFile = extractAccountFromFileName(
                file.getArchiveBlobUrl() != null ? new File(file.getArchiveBlobUrl()).getName() : null, 
                isMfcFile
        );

        String account = file.getAccountNumber() != null ? file.getAccountNumber() : accountFromFile;

        String key = file.getCustomerId() + "|" + account + "|" +
                (file.getArchiveBlobUrl() != null ? new File(file.getArchiveBlobUrl()).getName() : "");
        if (uniqueKeys.contains(key)) continue;
        uniqueKeys.add(key);

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(account);
        entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

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

        // --- Determine overall status for MFC / DEBTMAN ---
        boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
        boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
        boolean printSuccess = "SUCCESS".equals(entry.getPrintStatus());
        boolean archiveSuccess = "SUCCESS".equals(entry.getArchiveStatus());

        if (archiveSuccess) {
            if (emailSuccess || mobstatSuccess || printSuccess) {
                entry.setOverallStatus("SUCCESS");
            } else {
                entry.setOverallStatus("PARTIAL");
            }
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

// --- Updated helper for MFC ---
public static String extractAccountFromFileName(String fileName, boolean isMfcFile) {
    if (fileName == null || !fileName.contains("_")) return null;
    String[] parts = fileName.split("_");
    return isMfcFile ? parts[1] : parts[0]; // second part for MFC, first for normal
}
