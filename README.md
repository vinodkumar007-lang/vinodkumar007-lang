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
                .filter(e -> e.getKey().toLowerCase().contains(account.toLowerCase()))
                .map(Map.Entry::getValue)
                .toList();
    }
    
