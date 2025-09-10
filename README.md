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

    // Step 1: Upload all archive files and map them per account + filename
    Map<String, List<String>> archiveUrlsMap = new HashMap<>();
    Path archiveFolder = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    int archiveCount = 0;

    if (Files.exists(archiveFolder)) {
        try (Stream<Path> stream = Files.walk(archiveFolder)) {
            List<Path> archiveFiles = stream.filter(Files::isRegularFile)
                    .filter(f -> !isTempFile(f))
                    .toList();

            for (Path file : archiveFiles) {
                String fileName = file.getFileName().toString();
                for (SummaryProcessedFile customer : customerList) {
                    if (fileName.contains(customer.getAccountNumber())) {
                        String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                        archiveUrlsMap.computeIfAbsent(customer.getAccountNumber(), k -> new ArrayList<>()).add(blobUrl);
                        archiveCount++;
                    }
                }
            }
        }
    }
    logger.info("[{}] ✅ Total archive files uploaded: {}", msg.getBatchId(), archiveCount);

    // Step 2: Process other delivery folders
    for (String folder : deliveryFolders) {
        if (folder.equals(AppConstants.FOLDER_ARCHIVE)) continue; // already uploaded

        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) {
            logger.warn("[{}] ⚠️ Folder not found: {}", msg.getBatchId(), folderPath);
            continue;
        }

        int folderCount = 0;
        try (Stream<Path> stream = Files.walk(folderPath)) {
            List<Path> deliveryFiles = stream.filter(Files::isRegularFile)
                    .filter(f -> !isTempFile(f))
                    .toList();

            for (Path file : deliveryFiles) {
                String fileName = file.getFileName().toString();
                for (SummaryProcessedFile customer : customerList) {
                    if (fileName.contains(customer.getAccountNumber()) || fileName.endsWith(".ps")) {
                        String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);
                        folderCount++;

                        // Original delivery file entry
                        SummaryProcessedFile entry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, entry);
                        switch (folder) {
                            case AppConstants.FOLDER_EMAIL -> entry.setPdfEmailFileUrl(blobUrl);
                            case AppConstants.FOLDER_MOBSTAT -> entry.setPdfMobstatFileUrl(blobUrl);
                            case AppConstants.FOLDER_PRINT -> entry.setPrintFileUrl(blobUrl);
                        }

                        // Add matching archive URLs for the customer
                        List<String> archives = archiveUrlsMap.getOrDefault(customer.getAccountNumber(), Collections.emptyList());
                        for (String archiveUrl : archives) {
                            SummaryProcessedFile comboEntry = new SummaryProcessedFile();
                            BeanUtils.copyProperties(entry, comboEntry);
                            comboEntry.setArchiveBlobUrl(archiveUrl);
                            finalList.add(comboEntry);
                        }
                    }
                }
            }
        }
        logger.info("[{}] ✅ Total files uploaded in {} folder: {}", msg.getBatchId(), folder, folderCount);
    }

    logger.info("[{}] ✅ Total processed files (final summary entries): {}", msg.getBatchId(), finalList.size());
    return finalList;
}

// Helper: skip temp/hidden files
private boolean isTempFile(Path file) {
    String name = file.getFileName().toString().toLowerCase();
    return name.startsWith("~") || name.endsWith(".tmp") || name.endsWith(".temp") || name.equals(".ds_store");
}
