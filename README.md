/**
 * Builds a list of SummaryProcessedFile entries with archive/output blob URLs
 * and delivery status for each customer (EMAIL, MOBSTAT, PRINT).
 *
 * @param jobDir        Output job directory
 * @param customerList  Customer basic summary list
 * @param errorMap      Error report map (account -> method -> status)
 * @param msg           Kafka message for path info
 * @return List of SummaryProcessedFile objects with blob URLs and status
 */
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
                if (!Files.exists(file)) {
                    logger.warn("[{}] ⏩ Skipping missing archive file: {}", msg.getBatchId(), file);
                    return;
                }

                String fileName = file.getFileName().toString();
                String account = extractAccountFromFileName(fileName);
                if (account == null) {
                    logger.debug("[{}] ⚠️ No account number found in archive filename: {}", msg.getBatchId(), fileName);
                    return;
                }

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

    // -------- Upload delivery files --------
    Map<String, String> emailFileMap = new HashMap<>();
    Map<String, String> mobstatFileMap = new HashMap<>();
    Map<String, String> printFileMap = new HashMap<>();

    for (String folder : deliveryFolders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                if (!Files.exists(file)) {
                    logger.warn("[{}] ⏩ Skipping missing {} file: {}", msg.getBatchId(), folder, file);
                    return;
                }

                String fileName = file.getFileName().toString();
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

    // -------- Build final list --------
    Set<String> uniqueKeys = new HashSet<>();
    boolean isMfc = "MFC".equalsIgnoreCase(msg.getSourceSystem());

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;

        String account = customer.getAccountNumber();
        Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());

        for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
            String archiveFileName = archiveEntry.getKey();
            String archiveUrl = archiveEntry.getValue();

            String key = customer.getCustomerId() + "|" + account + "|" + archiveFileName;
            if (uniqueKeys.contains(key)) continue;
            uniqueKeys.add(key);

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setArchiveBlobUrl(archiveUrl);

            if (isMfc) {
                // 🔹 MFC: match delivery files by account anywhere in filename
                entry.setPdfEmailFileUrl(findFileByAccount(emailFileMap, account));
                entry.setPdfMobstatFileUrl(findFileByAccount(mobstatFileMap, account));
                entry.setPrintFileUrl(findFileByAccount(printFileMap, account));
            } else {
                // 🔹 DEBTMAN: first try exact filename, then fallback to account-based
                entry.setPdfEmailFileUrl(
                        emailFileMap.getOrDefault(archiveFileName, findFileByAccount(emailFileMap, account))
                );
                entry.setPdfMobstatFileUrl(
                        mobstatFileMap.getOrDefault(archiveFileName, findFileByAccount(mobstatFileMap, account))
                );
                entry.setPrintFileUrl(
                        printFileMap.getOrDefault(archiveFileName, findFileByAccount(printFileMap, account))
                );
            }

            finalList.add(entry);
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

/**
 * Extracts the account number from a given filename.
 * Matches if the account number appears anywhere in the filename.
 *
 * Examples:
 *   123_statement.pdf         -> 123
 *   abc_123_statement.pdf     -> 123
 *   statement_abc_123.pdf     -> 123
 *   statement123.pdf          -> 123
 */
private String extractAccountFromFileName(String fileName) {
    if (fileName == null || fileName.isBlank()) {
        return null;
    }

    // Remove extension first
    String baseName = fileName.contains(".")
            ? fileName.substring(0, fileName.lastIndexOf('.'))
            : fileName;

    // Look for a sequence of digits (min length 3 to avoid false matches)
    Pattern accountPattern = Pattern.compile("\\b(\\d{3,})\\b");
    Matcher matcher = accountPattern.matcher(baseName);

    if (matcher.find()) {
        return matcher.group(1);
    }

    return null; // no account number found
}

/**
 * Helper to match files by account number appearing anywhere in filename.
 */
private String findFileByAccount(Map<String, String> fileMap, String account) {
    if (account == null) return null;
    return fileMap.entrySet().stream()
            .filter(e -> e.getKey().contains(account))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
}
