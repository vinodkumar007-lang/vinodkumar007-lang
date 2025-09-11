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
                    String account = extractAccountFromFileName(fileName);
                    if (account == null) {
                        logger.warn("[{}] ⚠️ No account extracted from archive file: {}", msg.getBatchId(), fileName);
                        return;
                    }

                    try {
                        String archiveUrl = decodeUrl(
                                blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg)
                        );
                        accountToArchiveMap.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, archiveUrl);
                        logger.info("[{}] 📦 Uploaded archive file={} for account={}, url={}",
                                msg.getBatchId(), fileName, account, archiveUrl);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload archive file {}: {}",
                                msg.getBatchId(), fileName, e.getMessage(), e);
                    }
                });
            }
        } else {
            logger.warn("[{}] ⚠️ Archive folder does not exist: {}", msg.getBatchId(), archivePath);
        }

        // -------- Upload delivery files --------
        Map<String, String> emailFileMap = new HashMap<>();
        Map<String, String> mobstatFileMap = new HashMap<>();
        Map<String, String> printFileMap = new HashMap<>();

        for (String folder : deliveryFolders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) {
                logger.warn("[{}] ⚠️ Delivery folder not found: {}", msg.getBatchId(), folder);
                continue;
            }

            try (Stream<Path> stream = Files.walk(folderPath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
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
                        logger.info("[{}] ✅ Uploaded {} file: {} → {}",
                                msg.getBatchId(), folderToOutputMethod.get(folder), fileName, url);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload {} file {}: {}",
                                msg.getBatchId(), folderToOutputMethod.get(folder), fileName, e.getMessage(), e);
                    }
                });
            }
        }

        // -------- Build final list --------
        boolean isMfc = "MFC".equalsIgnoreCase(msg.getSourceSystem());
        boolean isDebtman = "DEBTMAN".equalsIgnoreCase(msg.getSourceSystem());

        for (SummaryProcessedFile customer : customerList) {
            if (customer == null || customer.getAccountNumber() == null) {
                logger.warn("[{}] ⏩ Skipping customer with null account: {}", msg.getBatchId(), customer);
                continue;
            }

            String account = customer.getAccountNumber();
            Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());

            if (archivesForAccount.isEmpty()) {
                logger.warn("[{}] ⚠️ No archive files found for customerId={}, account={}",
                        msg.getBatchId(), customer.getCustomerId(), account);
            }

            for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
                String archiveFileName = archiveEntry.getKey();
                String archiveUrl = archiveEntry.getValue();

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setArchiveBlobUrl(archiveUrl);

                if (isMfc || isDebtman) {
                    entry.setPdfEmailFileUrl(findFileByAccount(emailFileMap, account));
                    entry.setPdfMobstatFileUrl(findFileByAccount(mobstatFileMap, account));
                    entry.setPrintFileUrl(findFileByAccount(printFileMap, account));
                    logger.debug("[{}] Linked by account={} → email={}, mobstat={}, print={}",
                            msg.getBatchId(), account, entry.getPdfEmailFileUrl(),
                            entry.getPdfMobstatFileUrl(), entry.getPrintFileUrl());
                } else {
                    entry.setPdfEmailFileUrl(emailFileMap.get(archiveFileName));
                    entry.setPdfMobstatFileUrl(mobstatFileMap.get(archiveFileName));
                    entry.setPrintFileUrl(printFileMap.get(archiveFileName));
                    logger.debug("[{}] Linked by filename={} → email={}, mobstat={}, print={}",
                            msg.getBatchId(), archiveFileName, entry.getPdfEmailFileUrl(),
                            entry.getPdfMobstatFileUrl(), entry.getPrintFileUrl());
                }

                finalList.add(entry);
            }
        }

        // -------- Final summary log --------
        int archiveCount = accountToArchiveMap.values().stream().mapToInt(Map::size).sum();
        int emailCount = emailFileMap.size();
        int mobstatCount = mobstatFileMap.size();
        int printCount = printFileMap.size();
        int processedCount = finalList.size();

        logger.info("[{}] 📝 Summary stats → Archives={}, Email={}, Mobstat={}, Print={}, ProcessedEntries={}",
                msg.getBatchId(), archiveCount, emailCount, mobstatCount, printCount, processedCount);

        return finalList;
    }
