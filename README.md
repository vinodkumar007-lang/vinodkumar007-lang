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
            Map<String, Map<String, String>> errorMap
    ) throws IOException {

        Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();

        for (String folder : deliveryFolders) {
            deliveryFileMaps.put(folder, new HashMap<>());
            Path folderPath = jobDir.resolve(folder);

            logger.info("[{}] Scanning folder: {}", msg.getBatchId(), folderPath.toAbsolutePath());

            if (!Files.exists(folderPath)) {
                logger.warn("[{}] Folder does not exist: {}", msg.getBatchId(), folder);
                continue;
            }

            try (Stream<Path> stream = Files.walk(folderPath)) {
                stream.filter(Files::isRegularFile)
                        .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                        .forEach(file -> {
                            String fileName = file.getFileName().toString(); // only filename
                            logger.debug("[{}] Found file: {}", msg.getBatchId(), fileName);

                            try {
                                // Upload file and get the full URL
                                String uploadedUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);
                                String decodedUrl = decodeUrl(uploadedUrl);

                                // Store in map: key = filename, value = URL
                                deliveryFileMaps.get(folder).put(fileName, decodedUrl);

                                logger.info("[{}] ✅ Uploaded {} file: {}",
                                        msg.getBatchId(),
                                        folderToOutputMethod.getOrDefault(folder, folder),
                                        decodedUrl);

                            } catch (Exception e) {
                                logger.error("[{}] ⚠️ Failed to upload {} file {}: {}",
                                        msg.getBatchId(),
                                        folderToOutputMethod.getOrDefault(folder, folder),
                                        fileName,
                                        e.getMessage(), e);

                                errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                                        .put(fileName, folderToOutputMethod.getOrDefault(folder, folder) +
                                                " upload failed: " + e.getMessage());
                            }
                        });
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Error scanning folder {}: {}", msg.getBatchId(), folder, e.getMessage(), e);
            }

            logger.info("[{}] Total files uploaded in folder {}: {}", msg.getBatchId(), folder, deliveryFileMaps.get(folder).size());
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
     * Returns all delivery file URLs matching the account.
     */
    private List<String> findFilesByAccount(Map<String, String> fileMap, String account) {
        if (fileMap == null || fileMap.isEmpty() || account == null) {
            logger.warn("findFilesByAccount called with empty map or null account");
            return Collections.emptyList();
        }

        String normalizedAccount = account.trim().toLowerCase();

        // Log all available keys for debugging
        logger.debug("Available files in map: {}", fileMap.keySet());
        logger.debug("Searching for account: {}", normalizedAccount);

        List<String> matchedFiles = fileMap.entrySet().stream()
                .filter(e -> e.getKey() != null && e.getKey().toLowerCase().contains(normalizedAccount))
                .peek(e -> logger.debug("Matched file: {} -> {}", e.getKey(), e.getValue()))
                .map(Map.Entry::getValue)
                .toList();

        if (matchedFiles.isEmpty()) {
            logger.warn("No files matched for account: {}", account);
        } else {
            logger.info("Matched {} files for account: {}", matchedFiles.size(), account);
        }

        return matchedFiles;
    }
