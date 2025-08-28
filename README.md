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
                    // ✅ Skip temp/missing files
                    if (!Files.exists(file)) {
                        logger.warn("[{}] ⏩ Skipping missing archive file: {}", msg.getBatchId(), file);
                        return;
                    }

                    String fileName = file.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
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

        // -------- Upload delivery files --------
        Map<String, String> emailFileMap = new HashMap<>();
        Map<String, String> mobstatFileMap = new HashMap<>();
        Map<String, String> printFileMap = new HashMap<>();

        for (String folder : deliveryFolders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            try (Stream<Path> stream = Files.walk(folderPath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    // ✅ Skip temp/missing files
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

        boolean isMfc = AppConstants.SOURCE_MFC.equalsIgnoreCase(msg.getSourceSystem());

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
                    // 🔹 MFC: match delivery files by account
                    entry.setPdfEmailFileUrl(findFileByAccount(emailFileMap, account));
                    entry.setPdfMobstatFileUrl(findFileByAccount(mobstatFileMap, account));
                    entry.setPrintFileUrl(findFileByAccount(printFileMap, account));
                } else {
                    // 🔹 DEBTMAN: match delivery files by exact filename
                    entry.setPdfEmailFileUrl(emailFileMap.get(archiveFileName));
                    entry.setPdfMobstatFileUrl(mobstatFileMap.get(archiveFileName));
                    entry.setPrintFileUrl(printFileMap.get(archiveFileName));
                }

                finalList.add(entry);
            }
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }
    
    private String findFileByAccount(Map<String, String> fileMap, String account) {
        if (account == null) return null;
        return fileMap.entrySet().stream()
                .filter(e -> {
                    String fileName = e.getKey();
                    return fileName.startsWith(account + "_")               
                            || fileName.contains("_" + account + "_")      
                            || fileName.endsWith("_" + account + ".pdf");  
                })
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
