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

        // -------- Upload all archive files and map by account -> Map<filename, URL> --------
        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
        Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
        if (Files.exists(archivePath)) {
            try (Stream<Path> stream = Files.walk(archivePath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    String fileName = file.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
                    if (account == null) return;

                    try {
                        String archiveBlobUrl = decodeUrl(
                                blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg)
                        );
                        accountToArchiveFiles
                                .computeIfAbsent(account, k -> new HashMap<>())
                                .put(fileName, archiveBlobUrl);

                        logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveBlobUrl);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                    }
                });
            }
        }

        // -------- Upload delivery files and map by account -> Map<filename, URL> --------
        Map<String, Map<String, String>> emailFiles = new HashMap<>();
        Map<String, Map<String, String>> mobstatFiles = new HashMap<>();
        Map<String, Map<String, String>> printFiles = new HashMap<>();

        for (String folder : deliveryFolders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            try (Stream<Path> stream = Files.walk(folderPath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    String fileName = file.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
                    if (account == null) return;

                    try {
                        String deliveryUrl = decodeUrl(
                                blobStorageService.uploadFileByMessage(file.toFile(), folder, msg)
                        );

                        Map<String, Map<String, String>> targetMap;
                        switch (folder) {
                            case AppConstants.FOLDER_EMAIL -> targetMap = emailFiles;
                            case AppConstants.FOLDER_MOBSTAT -> targetMap = mobstatFiles;
                            case AppConstants.FOLDER_PRINT -> targetMap = printFiles;
                            default -> targetMap = null;
                        }

                        if (targetMap != null) {
                            targetMap.computeIfAbsent(account, k -> new HashMap<>())
                                    .put(fileName, deliveryUrl);
                            logger.info("[{}] ✅ Uploaded {} file for account {}: {}", msg.getBatchId(), folderToOutputMethod.get(folder), account, deliveryUrl);
                        }
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}", msg.getBatchId(), folderToOutputMethod.get(folder), account, e.getMessage(), e);
                    }
                });
            }
        }

        // -------- Build final list: One entry per archive file --------
        for (SummaryProcessedFile customer : customerList) {
            if (customer == null) continue;
            String account = customer.getAccountNumber();
            if (account == null || account.isBlank()) continue;

            Map<String, String> archiveFiles = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());

            // If no archive, still create one entry with delivery files
            if (archiveFiles.isEmpty()) {
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setPdfEmailFileUrl(emailFiles.getOrDefault(account, Collections.emptyMap()).values().stream().findFirst().orElse(null));
                entry.setPdfMobstatFileUrl(mobstatFiles.getOrDefault(account, Collections.emptyMap()).values().stream().findFirst().orElse(null));
                entry.setPrintFileUrl(printFiles.getOrDefault(account, Collections.emptyMap()).values().stream().findFirst().orElse(null));
                finalList.add(entry);
            } else {
                // One entry per archive file, match delivery file only if same filename exists
                for (Map.Entry<String, String> archiveEntry : archiveFiles.entrySet()) {
                    String archiveFileName = archiveEntry.getKey();
                    String archiveUrl = archiveEntry.getValue();

                    SummaryProcessedFile entry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, entry);
                    entry.setArchiveBlobUrl(archiveUrl);

                    entry.setPdfEmailFileUrl(emailFiles.getOrDefault(account, Collections.emptyMap()).get(archiveFileName));
                    entry.setPdfMobstatFileUrl(mobstatFiles.getOrDefault(account, Collections.emptyMap()).get(archiveFileName));
                    entry.setPrintFileUrl(printFiles.getOrDefault(account, Collections.emptyMap()).get(archiveFileName));

                    finalList.add(entry);
                }
            }
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }

// Helper: implement proper logic to extract account from file name
    private String extractAccountFromFileName(String fileName) {
        // Example: 1002444400101_LHDLR02E.pdf => account = 1002444400101
        if (fileName == null || !fileName.contains("_")) return null;
        return fileName.split("_")[0];
    }
