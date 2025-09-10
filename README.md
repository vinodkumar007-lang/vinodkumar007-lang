private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    if (customerList == null || customerList.isEmpty() || jobDir == null || !Files.exists(jobDir)) {
        logger.warn("[{}] ⚠️ Job directory or customer list empty", msg.getBatchId());
        return finalList;
    }

    List<String> deliveryFolders = List.of(
            AppConstants.FOLDER_ARCHIVE,
            AppConstants.FOLDER_EMAIL,
            AppConstants.FOLDER_MOBSTAT,
            AppConstants.FOLDER_PRINT
    );

    // Map for quick account-based matching
    Map<String, SummaryProcessedFile> accountMap = customerList.stream()
            .collect(Collectors.toMap(SummaryProcessedFile::getAccountNumber, Function.identity()));

    // Load all archive files first for combination
    Map<String, Path> archiveFiles = new HashMap<>();
    Path archiveFolder = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    if (Files.exists(archiveFolder)) {
        try (Stream<Path> stream = Files.walk(archiveFolder)) {
            stream.filter(Files::isRegularFile).forEach(f -> {
                String fileName = f.getFileName().toString();
                for (String account : accountMap.keySet()) {
                    if (fileName.contains(account)) {
                        archiveFiles.put(account + "_" + fileName, f);
                    }
                }
            });
        }
    }

    // Process each delivery folder
    for (String folder : deliveryFolders) {
        Path folderPath = jobDir.resolve(folder);

        if (!Files.exists(folderPath)) {
            logger.warn("[{}] ⚠️ Folder not found: {}", msg.getBatchId(), folderPath);
            continue;
        }

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                try {
                    String fileName = file.getFileName().toString();

                    // Upload to blob
                    String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);
                    logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folder, blobUrl);

                    // Match account in filename
                    for (Map.Entry<String, SummaryProcessedFile> entry : accountMap.entrySet()) {
                        String account = entry.getKey();
                        SummaryProcessedFile customerEntry = entry.getValue();

                        boolean matchesAccount = fileName.contains(account) || fileName.endsWith(".ps");

                        if (matchesAccount) {
                            // Original file entry
                            SummaryProcessedFile singleFileEntry = new SummaryProcessedFile();
                            BeanUtils.copyProperties(customerEntry, singleFileEntry);

                            switch (folder) {
                                case AppConstants.FOLDER_ARCHIVE -> singleFileEntry.setArchiveFileUrl(blobUrl);
                                case AppConstants.FOLDER_EMAIL -> singleFileEntry.setPdfEmailFileUrl(blobUrl);
                                case AppConstants.FOLDER_MOBSTAT -> singleFileEntry.setPdfMobstatFileUrl(blobUrl);
                                case AppConstants.FOLDER_PRINT -> singleFileEntry.setPrintFileUrl(blobUrl);
                            }

                            finalList.add(singleFileEntry);

                            // Add archive + delivery combinations (skip if already archive)
                            if (!folder.equals(AppConstants.FOLDER_ARCHIVE)) {
                                archiveFiles.entrySet().stream()
                                        .filter(a -> a.getKey().startsWith(account))
                                        .forEach(a -> {
                                            try {
                                                String archiveBlobUrl = blobStorageService.uploadFileByMessage(a.getValue().toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                                                SummaryProcessedFile comboEntry = new SummaryProcessedFile();
                                                BeanUtils.copyProperties(customerEntry, comboEntry);

                                                // Set delivery + archive
                                                switch (folder) {
                                                    case AppConstants.FOLDER_EMAIL -> comboEntry.setPdfEmailFileUrl(blobUrl);
                                                    case AppConstants.FOLDER_MOBSTAT -> comboEntry.setPdfMobstatFileUrl(blobUrl);
                                                    case AppConstants.FOLDER_PRINT -> comboEntry.setPrintFileUrl(blobUrl);
                                                }
                                                comboEntry.setArchiveFileUrl(archiveBlobUrl);
                                                finalList.add(comboEntry);
                                            } catch (Exception e) {
                                                logger.error("[{}] ⚠️ Failed archive combo upload: {}", msg.getBatchId(), e.getMessage());
                                            }
                                        });
                            }
                        }
                    }

                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to process file {}: {}", msg.getBatchId(), file.getFileName(), e.getMessage());
                }
            });
        }
    }

    logger.info("[{}] ✅ Total processed files: {}", msg.getBatchId(), finalList.size());
    return finalList;
}
