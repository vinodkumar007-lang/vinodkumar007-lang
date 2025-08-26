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

        // -------- Upload all archive files and map by filename --------
        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
        Map<String, String> archiveFileMap = new HashMap<>();
        if (Files.exists(archivePath)) {
            try (Stream<Path> stream = Files.walk(archivePath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    String fileName = file.getFileName().toString();
                    try {
                        String archiveUrl = decodeUrl(
                                blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg)
                        );
                        archiveFileMap.put(fileName, archiveUrl);
                        logger.info("[{}] 📦 Uploaded archive file: {}", msg.getBatchId(), archiveUrl);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                    }
                });
            }
        }

        // -------- Upload delivery files and map by filename --------
        Map<String, String> emailFileMap = new HashMap<>();
        Map<String, String> mobstatFileMap = new HashMap<>();
        Map<String, String> printFileMap = new HashMap<>();

        for (String folder : deliveryFolders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

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
                        logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folderToOutputMethod.get(folder), url);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(), folderToOutputMethod.get(folder), fileName, e.getMessage(), e);
                    }
                });
            }
        }

        // -------- Build final list: prevent duplicates --------
        Set<String> uniqueKeys = new HashSet<>(); // customerId + accountNumber + archiveFilename

        for (SummaryProcessedFile customer : customerList) {
            if (customer == null) continue;

            for (Map.Entry<String, String> archiveEntry : archiveFileMap.entrySet()) {
                String archiveFileName = archiveEntry.getKey();
                String archiveUrl = archiveEntry.getValue();

                String key = customer.getCustomerId() + "|" + customer.getAccountNumber() + "|" + archiveFileName;
                if (uniqueKeys.contains(key)) continue;
                uniqueKeys.add(key);

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setArchiveBlobUrl(archiveUrl);

                // Match delivery files only if filename matches
                entry.setPdfEmailFileUrl(emailFileMap.get(archiveFileName));
                entry.setPdfMobstatFileUrl(mobstatFileMap.get(archiveFileName));
                entry.setPrintFileUrl(printFileMap.get(archiveFileName));

                finalList.add(entry);
            }
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }
