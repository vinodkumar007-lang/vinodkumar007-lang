/**
 * Builds a list of SummaryProcessedFile entries with archive/output blob URLs
 * and delivery status for each customer (EMAIL, MOBSTAT, PRINT).
 */
private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    if (!validateInputs(jobDir, customerList, msg)) {
        return finalList;
    }

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

    // Step 1: Upload all archive files
    Map<String, Map<String, String>> accountToArchiveMap = uploadArchiveFiles(jobDir, msg, errorMap);

    // Step 2: Upload all delivery files (EMAIL, MOBSTAT, PRINT)
    Map<String, Map<String, String>> deliveryFileMaps = uploadDeliveryFiles(jobDir, deliveryFolders, folderToOutputMethod, msg, errorMap);

    // Step 3: Build final processed list combining archive + delivery files
    finalList = buildFinalProcessedList(customerList, accountToArchiveMap, deliveryFileMaps, msg);

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

private boolean validateInputs(Path jobDir, List<SummaryProcessedFile> customerList, KafkaMessage msg) {
    if (jobDir == null) {
        logger.warn("[{}] ⚠️ jobDir is null. Skipping buildDetailedProcessedFiles.", msg != null ? msg.getBatchId() : "UNKNOWN");
        return false;
    }
    if (customerList == null || customerList.isEmpty()) {
        logger.warn("[{}] ⚠️ customerList is null/empty. Nothing to process.", msg != null ? msg.getBatchId() : "UNKNOWN");
        return false;
    }
    if (msg == null) {
        logger.warn("[UNKNOWN] ⚠️ KafkaMessage is null. Skipping buildDetailedProcessedFiles.");
        return false;
    }
    return true;
}

/**
 * Upload all archive files
 */
private Map<String, Map<String, String>> uploadArchiveFiles(Path jobDir, KafkaMessage msg, Map<String, Map<String, String>> errorMap) throws IOException {
    Map<String, Map<String, String>> accountToArchiveMap = new HashMap<>();
    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);

    if (!Files.exists(archivePath)) {
        logger.warn("[{}] ⚠️ Archive folder does not exist: {}", msg.getBatchId(), archivePath);
        return accountToArchiveMap;
    }

    try (Stream<Path> stream = Files.walk(archivePath)) {
        stream.filter(Files::isRegularFile)
                .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                .forEach(file -> processArchiveFile(file, msg, accountToArchiveMap, errorMap));
    }

    return accountToArchiveMap;
}

private void processArchiveFile(Path file, KafkaMessage msg,
                                Map<String, Map<String, String>> accountToArchiveMap,
                                Map<String, Map<String, String>> errorMap) {
    if (!Files.exists(file)) return;

    String fileName = file.getFileName().toString();
    String account = extractAccountFromFileName(fileName);
    if (account == null) return;

    try {
        String archiveUrl = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg));
        accountToArchiveMap.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, archiveUrl);
        logger.info("[{}] 📦 Uploaded archive file for account [{}]: {}", msg.getBatchId(), account, archiveUrl);
    } catch (Exception e) {
        logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
        errorMap.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, "Archive upload failed: " + e.getMessage());
    }
}

/**
 * Upload all delivery files in given folders
 */
private Map<String, Map<String, String>> uploadDeliveryFiles(
        Path jobDir,
        List<String> deliveryFolders,
        Map<String, String> folderToOutputMethod,
        KafkaMessage msg,
        Map<String, Map<String, String>> errorMap) throws IOException {

    Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();
    for (String folder : deliveryFolders) {
        deliveryFileMaps.put(folder, new HashMap<>());
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                    .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                    .forEach(file -> {
                        String fileName = file.getFileName().toString();
                        try {
                            String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), folder, msg));
                            deliveryFileMaps.get(folder).put(fileName, url);
                            logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folderToOutputMethod.get(folder), url);
                        } catch (Exception e) {
                            logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(),
                                    folderToOutputMethod.getOrDefault(folder, folder), fileName, e.getMessage(), e);
                            errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                                    .put(fileName, folderToOutputMethod.getOrDefault(folder, folder) + " upload failed: " + e.getMessage());
                        }
                    });
        }
    }
    return deliveryFileMaps;
}

/**
 * Build final processed list combining archive + delivery files
 * Creates separate entry for each delivery file if multiple exist
 */
private List<SummaryProcessedFile> buildFinalProcessedList(
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> accountToArchiveMap,
        Map<String, Map<String, String>> deliveryFileMaps,
        KafkaMessage msg) {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    Set<String> uniqueKeys = new HashSet<>();
    boolean isMfc = AppConstants.SOURCE_MFC.equalsIgnoreCase(msg.getSourceSystem());

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;

        String account = customer.getAccountNumber();
        Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());

        for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
            String archiveFileName = archiveEntry.getKey();
            String archiveUrl = archiveEntry.getValue();

            List<String> emailFiles = findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL), account);
            List<String> mobstatFiles = findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT), account);
            List<String> printFiles = findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_PRINT), account);

            if (emailFiles.isEmpty() && mobstatFiles.isEmpty() && printFiles.isEmpty()) {
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setArchiveBlobUrl(archiveUrl);
                finalList.add(entry);
                continue;
            }

            int maxFiles = Math.max(emailFiles.size(), Math.max(mobstatFiles.size(), printFiles.size()));
            for (int i = 0; i < maxFiles; i++) {
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setArchiveBlobUrl(archiveUrl);

                entry.setPdfEmailFileUrl(i < emailFiles.size() ? emailFiles.get(i) : null);
                entry.setPdfMobstatFileUrl(i < mobstatFiles.size() ? mobstatFiles.get(i) : null);
                entry.setPrintFileUrl(i < printFiles.size() ? printFiles.get(i) : null);

                String key = customer.getCustomerId() + "|" + account + "|" + archiveFileName
                        + "|" + entry.getPdfEmailFileUrl() + "|" + entry.getPdfMobstatFileUrl() + "|" + entry.getPrintFileUrl();
                if (!uniqueKeys.add(key)) continue;

                finalList.add(entry);
            }
        }
    }
    return finalList;
}

/**
 * Returns all delivery file URLs matching the account
 */
private List<String> findFilesByAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null || fileMap.isEmpty() || account == null) return Collections.emptyList();
    return fileMap.entrySet().stream()
            .filter(e -> {
                String fileName = e.getKey();
                return fileName.startsWith(account + "_")
                        || fileName.contains("_" + account + "_")
                        || fileName.endsWith("_" + account + ".pdf");
            })
            .map(Map.Entry::getValue)
            .toList();
}
