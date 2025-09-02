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

    // Step 2: Upload delivery files
    Map<String, Map<String, String>> deliveryFileMaps = uploadDeliveryFiles(jobDir, deliveryFolders, folderToOutputMethod, msg, errorMap);

    // Step 3: Build final list
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

private Map<String, Map<String, String>> uploadDeliveryFiles(
        Path jobDir,
        List<String> deliveryFolders,
        Map<String, String> folderToOutputMethod,
        KafkaMessage msg,
        Map<String, Map<String, String>> errorMap) throws IOException {

    Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();
    deliveryFileMaps.put(AppConstants.FOLDER_EMAIL, new HashMap<>());
    deliveryFileMaps.put(AppConstants.FOLDER_MOBSTAT, new HashMap<>());
    deliveryFileMaps.put(AppConstants.FOLDER_PRINT, new HashMap<>());

    for (String folder : deliveryFolders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) {
            logger.debug("[{}] ℹ️ Delivery folder not found: {}", msg.getBatchId(), folder);
            continue;
        }

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                    .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                    .forEach(file -> processDeliveryFile(file, folder, folderToOutputMethod, msg, deliveryFileMaps, errorMap));
        }
    }

    return deliveryFileMaps;
}

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

private List<SummaryProcessedFile> buildFinalProcessedList(
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> accountToArchiveMap,
        Map<String, Map<String, String>> deliveryFileMaps,
        KafkaMessage msg) {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    Set<String> uniqueKeys = new HashSet<>();
    boolean isMfc = AppConstants.SOURCE_MFC.equalsIgnoreCase(msg.getSourceSystem());

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) {
            logger.debug("[{}] ⏩ Skipping null/invalid customer entry.", msg.getBatchId());
            continue;
        }

        String account = customer.getAccountNumber();
        Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());

        for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
            String archiveFileName = archiveEntry.getKey();
            String archiveUrl = archiveEntry.getValue();

            String key = customer.getCustomerId() + "|" + account + "|" + archiveFileName;
            if (!uniqueKeys.add(key)) {
                logger.debug("[{}] ⏩ Duplicate entry skipped for key={}", msg.getBatchId(), key);
                continue;
            }

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setArchiveBlobUrl(archiveUrl);

            if (isMfc) {
                entry.setPdfEmailFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL), account));
                entry.setPdfMobstatFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT), account));
                entry.setPrintFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_PRINT), account));
            } else {
                entry.setPdfEmailFileUrl(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL).get(archiveFileName));
                entry.setPdfMobstatFileUrl(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT).get(archiveFileName));
                entry.setPrintFileUrl(deliveryFileMaps.get(AppConstants.FOLDER_PRINT).get(archiveFileName));
            }

            finalList.add(entry);
        }
    }
    return finalList;
}

I’ve refactored buildDetailedProcessedFiles into the following helper method
