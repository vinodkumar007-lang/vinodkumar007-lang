private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
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

    // Step 1: Pre-upload all archive files once
    Map<String, List<String>> archiveBlobUrlsMap = new HashMap<>();
    Path archiveFolder = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    if (Files.exists(archiveFolder) && Files.isDirectory(archiveFolder)) {
        try (Stream<Path> stream = Files.walk(archiveFolder)) {
            stream.filter(Files::isRegularFile)
                    .filter(f -> !isTempFile(f))
                    .forEach(f -> {
                        String fileName = f.getFileName().toString();
                        customerList.forEach(cust -> {
                            if (fileName.contains(cust.getAccountNumber())) {
                                try {
                                    String blobUrl = blobStorageService.uploadFileByMessage(f.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                                    archiveBlobUrlsMap.computeIfAbsent(cust.getAccountNumber(), k -> new ArrayList<>()).add(blobUrl);
                                    logger.info("[{}] ✅ Pre-uploaded archive file for account {}: {}", msg.getBatchId(), cust.getAccountNumber(), blobUrl);
                                } catch (Exception e) {
                                    logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), f.getFileName(), e.getMessage());
                                }
                            }
                        });
                    });
        }
    } else {
        logger.warn("[{}] ⚠️ Archive folder missing: {}", msg.getBatchId(), archiveFolder);
    }

    // Step 2: Process each delivery folder
    for (String folder : deliveryFolders) {
        Path folderPath = jobDir.resolve(folder);
        logger.info("[{}] 🔍 Checking folder: {}", msg.getBatchId(), folderPath.toAbsolutePath());

        if (!Files.exists(folderPath) || !Files.isDirectory(folderPath)) {
            logger.warn("[{}] ⚠️ Folder not found: {}", msg.getBatchId(), folderPath);
            continue;
        }

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                    .filter(f -> !isTempFile(f))
                    .forEach(file -> {
                        try {
                            String fileName = file.getFileName().toString();
                            String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);
                            logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folder, blobUrl);

                            // Match accounts in filename
                            customerList.forEach(customerEntry -> {
                                boolean matchesAccount = fileName.contains(customerEntry.getAccountNumber()) || fileName.endsWith(".ps");
                                if (matchesAccount) {
                                    // Original entry
                                    SummaryProcessedFile entry = new SummaryProcessedFile();
                                    BeanUtils.copyProperties(customerEntry, entry);
                                    switch (folder) {
                                        case AppConstants.FOLDER_ARCHIVE -> entry.setArchiveBlobUrl(blobUrl);
                                        case AppConstants.FOLDER_EMAIL -> entry.setPdfEmailFileUrl(blobUrl);
                                        case AppConstants.FOLDER_MOBSTAT -> entry.setPdfMobstatFileUrl(blobUrl);
                                        case AppConstants.FOLDER_PRINT -> entry.setPrintFileUrl(blobUrl);
                                    }
                                    finalList.add(entry);

                                    // Archive + delivery combos if folder != archive
                                    if (!folder.equals(AppConstants.FOLDER_ARCHIVE)) {
                                        List<String> archives = archiveBlobUrlsMap.getOrDefault(customerEntry.getAccountNumber(), Collections.emptyList());
                                        for (String archiveBlobUrl : archives) {
                                            SummaryProcessedFile comboEntry = new SummaryProcessedFile();
                                            BeanUtils.copyProperties(customerEntry, comboEntry);
                                            switch (folder) {
                                                case AppConstants.FOLDER_EMAIL -> comboEntry.setPdfEmailFileUrl(blobUrl);
                                                case AppConstants.FOLDER_MOBSTAT -> comboEntry.setPdfMobstatFileUrl(blobUrl);
                                                case AppConstants.FOLDER_PRINT -> comboEntry.setPrintFileUrl(blobUrl);
                                            }
                                            comboEntry.setArchiveBlobUrl(archiveBlobUrl);
                                            finalList.add(comboEntry);
                                        }
                                    }
                                }
                            });
                        } catch (Exception e) {
                            logger.error("[{}] ⚠️ Failed to process file {}: {}", msg.getBatchId(), file.getFileName(), e.getMessage());
                        }
                    });
        }
    }

    logger.info("[{}] ✅ Total processed files: {}", msg.getBatchId(), finalList.size());
    return finalList;
}

// Helper method: Skip temp/hidden files
private boolean isTempFile(Path file) {
    String name = file.getFileName().toString().toLowerCase();
    return name.startsWith("~") || name.endsWith(".tmp") || name.endsWith(".temp") || name.equals(".ds_store");
}
