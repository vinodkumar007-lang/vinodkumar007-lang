/**
 * Uploads all files from available folders (ARCHIVE, EMAIL, PRINT, MOBSTAT).
 */
private Map<String, Map<String, String>> uploadAllFiles(Path jobDir,
                                                        KafkaMessage msg,
                                                        Map<String, Map<String, String>> errorMap) throws IOException {
    Map<String, Map<String, String>> accountToFiles = new HashMap<>();

    // list of all supported folders
    List<String> folders = Arrays.asList(
            AppConstants.FOLDER_ARCHIVE,
            AppConstants.FOLDER_EMAIL,
            AppConstants.FOLDER_PRINT,
            AppConstants.FOLDER_MOBSTAT
    );

    for (String folder : folders) {
        Map<String, Map<String, String>> uploaded = uploadFilesByFolder(jobDir, folder, msg, errorMap);

        // merge uploaded results into master map
        uploaded.forEach((acct, files) ->
                accountToFiles.merge(acct, files, (oldMap, newMap) -> {
                    oldMap.putAll(newMap);
                    return oldMap;
                })
        );
    }

    return accountToFiles;
}

/**
 * Upload files from a specific folder under jobDir.
 */
private Map<String, Map<String, String>> uploadFilesByFolder(Path jobDir,
                                                             String folderName,
                                                             KafkaMessage msg,
                                                             Map<String, Map<String, String>> errorMap) throws IOException {
    Map<String, Map<String, String>> accountToFileMap = new HashMap<>();
    Path targetPath = jobDir.resolve(folderName);

    if (!Files.exists(targetPath)) {
        logger.warn("[{}] ⚠️ Folder does not exist: {}", msg.getBatchId(), targetPath);
        return accountToFileMap;
    }

    try (Stream<Path> stream = Files.walk(targetPath)) {
        stream.filter(Files::isRegularFile)
              .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
              .forEach(file -> processFileByFolder(file, folderName, msg, accountToFileMap, errorMap));
    }

    return accountToFileMap;
}

/**
 * Process a single file and upload it to Blob.
 */
private void processFileByFolder(Path file,
                                 String folderName,
                                 KafkaMessage msg,
                                 Map<String, Map<String, String>> accountToFileMap,
                                 Map<String, Map<String, String>> errorMap) {
    if (!Files.exists(file)) {
        logger.warn("[{}] ⏩ Skipping missing {} file: {}", msg.getBatchId(), folderName, file);
        return;
    }

    String fileName = file.getFileName().toString();
    String account = extractAccountFromFileName(fileName);
    if (account == null) {
        logger.debug("[{}] ⚠️ Skipping {} file without account mapping: {}", msg.getBatchId(), folderName, fileName);
        return;
    }

    try {
        String blobUrl = decodeUrl(
                blobStorageService.uploadFileByMessage(file.toFile(), folderName, msg)
        );

        accountToFileMap
                .computeIfAbsent(account, k -> new HashMap<>())
                .put(fileName, blobUrl);

        logger.info("[{}] 📦 Uploaded {} file for account [{}]: {}", msg.getBatchId(), folderName, account, blobUrl);
    } catch (Exception e) {
        logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(), folderName, fileName, e.getMessage(), e);
        errorMap.computeIfAbsent(account, k -> new HashMap<>())
                .put(fileName, folderName + " upload failed: " + e.getMessage());
    }
}
