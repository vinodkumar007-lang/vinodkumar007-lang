/**
 * Main method to build detailed processed files including archive and delivery files.
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

    // Step 1: Upload archive files
    Map<String, Map<String, String>> accountToArchiveMap = uploadArchiveFiles(jobDir, msg, errorMap);

    // Step 2: Upload delivery files (case-insensitive folder match)
    Map<String, Map<String, String>> deliveryFileMaps = uploadDeliveryFiles(jobDir, deliveryFolders, folderToOutputMethod, msg, errorMap);

    // Step 3: Build final processed list including delivery-only entries
    finalList = buildFinalProcessedList(customerList, accountToArchiveMap, deliveryFileMaps, msg);

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

/**
 * Validates inputs before processing.
 */
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
 * Uploads all archive files under jobDir/FOLDER_ARCHIVE
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

/**
 * Processes and uploads all archive files per account.
 * Supports multiple files with different suffixes for the same account.
 */
private void processArchiveFile(Path file, KafkaMessage msg,
                                Map<String, Map<String, String>> accountToArchiveMap,
                                Map<String, Map<String, String>> errorMap) {
    if (!Files.exists(file)) {
        logger.warn("[{}] ⏩ Skipping missing archive file: {}", msg.getBatchId(), file);
        return;
    }

    String fileName = file.getFileName().toString();
    String account = extractAccountFromFileName(fileName);
    if (account == null) {
        logger.debug("[{}] ⚠️ Skipping archive file without account mapping: {}", msg.getBatchId(), fileName);
        return;
    }

    try {
        String archiveUrl = decodeUrl(
                blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg)
        );

        // Add to existing map, supporting multiple files per account
        accountToArchiveMap
                .computeIfAbsent(account, k -> new HashMap<>())
                .put(fileName, archiveUrl);

        logger.info("[{}] 📦 Uploaded archive file for account [{}]: {}", msg.getBatchId(), account, archiveUrl);
    } catch (Exception e) {
        logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
        errorMap.computeIfAbsent(account, k -> new HashMap<>())
                .put(fileName, "Archive upload failed: " + e.getMessage());
    }
}

/**
 * Uploads delivery files (email, mobstat, print) under jobDir.
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
    }

    try (Stream<Path> allDirs = Files.walk(jobDir)) {
        List<Path> folderPaths = allDirs
                .filter(Files::isDirectory)
                .filter(p -> deliveryFolders.stream()
                        .anyMatch(f -> f.equalsIgnoreCase(p.getFileName().toString()))) // Case-insensitive
                .toList();

        if (folderPaths.isEmpty()) {
            logger.warn("[{}] ⚠️ No delivery folders found under jobDir: {}", msg.getBatchId(), jobDir);
            return deliveryFileMaps;
        }

        for (Path folderPath : folderPaths) {
            String folderName = folderPath.getFileName().toString();
            logger.info("[{}] 🔎 Processing delivery folder: {}", msg.getBatchId(), folderPath);

            try (Stream<Path> files = Files.walk(folderPath)) {
                files.filter(Files::isRegularFile)
                        .filter(f -> !f.getFileName().toString().endsWith(".tmp"))
                        .forEach(file -> {
                            logger.info("[{}] 📂 Found file in folder [{}]: {}", msg.getBatchId(), folderName, file.getFileName());
                            processDeliveryFile(file, folderName, folderToOutputMethod, msg, deliveryFileMaps, errorMap);
                        });
            }
        }
    }

    return deliveryFileMaps;
}

/**
 * Processes a single delivery file and uploads to blob storage.
 */
private void processDeliveryFile(Path file, String folder,
                                 Map<String, String> folderToOutputMethod,
                                 KafkaMessage msg,
                                 Map<String, Map<String, String>> deliveryFileMaps,
                                 Map<String, Map<String, String>> errorMap) {
    if (!Files.exists(file)) {
        logger.warn("[{}] ⏩ Skipping missing {} file: {}", msg.getBatchId(), folder, file);
        return;
    }

    String fileName = file.getFileName().toString();
    try {
        String url = decodeUrl(
                blobStorageService.uploadFileByMessage(file.toFile(), folder, msg)
        );
        deliveryFileMaps.get(folder).put(fileName, url);

        logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(),
                folderToOutputMethod.get(folder), url);
    } catch (Exception e) {
        logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(),
                folderToOutputMethod.getOrDefault(folder, folder), fileName, e.getMessage(), e);
        errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                .put(fileName, folderToOutputMethod.getOrDefault(folder, folder) + " upload failed: " + e.getMessage());
    }
}

/**
 * Builds final processed list combining archive and delivery files.
 * Picks all files for same account (including different suffixes).
 */
private List<SummaryProcessedFile> buildFinalProcessedList(
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> accountToArchiveMap,
        Map<String, Map<String, String>> deliveryFileMaps,
        KafkaMessage msg) {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    boolean isMfc = AppConstants.SOURCE_MFC.equalsIgnoreCase(msg.getSourceSystem());

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        SummaryProcessedFile entry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, entry);

        // Multiple archive files combined
        Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());
        entry.setArchiveBlobUrl(String.join(",", archivesForAccount.values()));

        // Multiple delivery files combined
        entry.setPdfEmailFileUrl(String.join(",", findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL), account)));
        entry.setPdfMobstatFileUrl(String.join(",", findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT), account)));
        entry.setPrintFileUrl(String.join(",", findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_PRINT), account)));

        finalList.add(entry);
    }

    logger.info("[{}] ✅ buildFinalProcessedList completed. Total entries={}", msg.getBatchId(), finalList.size());
    return finalList;
}

/**
 * Returns all uploaded files for an account including different suffixes.
 */
private List<String> findFilesByAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null || fileMap.isEmpty() || account == null) return Collections.emptyList();

    return fileMap.entrySet().stream()
            .filter(e -> e.getKey().contains(account))
            .map(Map.Entry::getValue)
            .toList();
}
