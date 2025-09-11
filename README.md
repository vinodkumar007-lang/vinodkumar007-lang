private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

    List<String> allFolders = List.of(
            AppConstants.FOLDER_ARCHIVE,
            AppConstants.FOLDER_EMAIL,
            AppConstants.FOLDER_MOBSTAT,
            AppConstants.FOLDER_PRINT
    );

    Map<String, String> folderToOutputMethod = Map.of(
            AppConstants.FOLDER_EMAIL, AppConstants.OUTPUT_EMAIL,
            AppConstants.FOLDER_MOBSTAT, AppConstants.OUTPUT_MOBSTAT,
            AppConstants.FOLDER_PRINT, AppConstants.OUTPUT_PRINT,
            AppConstants.FOLDER_ARCHIVE, AppConstants.OUTPUT_ARCHIVE
    );

    // -------- Map files by account number --------
    Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();

    for (String folder : allFolders) {
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
                    String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), folder, msg));

                    // ✅ Match file to each customer by account number
                    for (SummaryProcessedFile customer : customerList) {
                        if (customer == null || customer.getAccountNumber() == null) continue;
                        String account = customer.getAccountNumber();
                        if (!fileName.contains(account)) continue;

                        switch (folder) {
                            case AppConstants.FOLDER_ARCHIVE ->
                                    accountToArchiveFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            case AppConstants.FOLDER_EMAIL ->
                                    accountToEmailFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            case AppConstants.FOLDER_MOBSTAT ->
                                    accountToMobstatFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            case AppConstants.FOLDER_PRINT ->
                                    accountToPrintFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        }

                        logger.info("[{}] ✅ Uploaded {} file={} for account={}, url={}", msg.getBatchId(),
                                folderToOutputMethod.get(folder), fileName, account, url);
                    }

                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(),
                            folderToOutputMethod.get(folder), fileName, e.getMessage(), e);
                }
            });
        }
    }

    // -------- Build final list using customerList --------
    Set<String> uniqueKeys = new HashSet<>();

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        Map<String, String> archivesForAccount = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());

        for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
            String archiveFileName = archiveEntry.getKey();
            String archiveUrl = archiveEntry.getValue();

            String key = customer.getCustomerId() + "|" + account + "|" + archiveFileName;
            if (uniqueKeys.contains(key)) continue;
            uniqueKeys.add(key);

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setArchiveBlobUrl(archiveUrl);

            // ✅ Assign delivery URLs by account number
            entry.setPdfEmailFileUrl(accountToEmailFiles.getOrDefault(account, Collections.emptyMap())
                    .values().stream().findFirst().orElse(null));
            entry.setPdfMobstatFileUrl(accountToMobstatFiles.getOrDefault(account, Collections.emptyMap())
                    .values().stream().findFirst().orElse(null));
            entry.setPrintFileUrl(accountToPrintFiles.getOrDefault(account, Collections.emptyMap())
                    .values().stream().findFirst().orElse(null));

            finalList.add(entry);
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}
