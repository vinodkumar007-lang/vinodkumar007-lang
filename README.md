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
            AppConstants.FOLDER_MOBSTAT
    );

    // Step 1: Upload all archive files and map per account+filename
    Map<String, List<String>> archiveUrlsMap = new HashMap<>();
    Path archiveFolder = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    int archiveCount = 0;

    if (Files.exists(archiveFolder)) {
        try (Stream<Path> stream = Files.walk(archiveFolder)) {
            for (Path file : stream.filter(Files::isRegularFile)
                                   .filter(f -> !isTempFile(f))
                                   .toList()) {
                String fileName = file.getFileName().toString();
                for (SummaryProcessedFile customer : customerList) {
                    if (fileName.contains(customer.getAccountNumber())) {
                        String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                        archiveUrlsMap.computeIfAbsent(customer.getAccountNumber(), k -> new ArrayList<>()).add(blobUrl);

                        SummaryProcessedFile entry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, entry);
                        entry.setArchiveBlobUrl(blobUrl);
                        finalList.add(entry);
                        archiveCount++;
                    }
                }
            }
        }
    }
    logger.info("[{}] ✅ Uploaded {} archive files", msg.getBatchId(), archiveCount);

    // Step 2: Upload other delivery files and combine with all relevant archives
    Map<String, Integer> folderCounts = new HashMap<>();
    for (String folder : deliveryFolders) {
        if (folder.equals(AppConstants.FOLDER_ARCHIVE)) continue; // Already handled

        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) {
            logger.warn("[{}] ⚠️ Folder not found: {}", msg.getBatchId(), folderPath);
            continue;
        }

        int count = 0;
        try (Stream<Path> stream = Files.walk(folderPath)) {
            for (Path file : stream.filter(Files::isRegularFile)
                                   .filter(f -> !isTempFile(f))
                                   .toList()) {
                String fileName = file.getFileName().toString();
                for (SummaryProcessedFile customer : customerList) {
                    boolean matchesAccount = fileName.contains(customer.getAccountNumber()) || fileName.endsWith(".ps");
                    if (matchesAccount) {
                        String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);

                        // Combine with all archives for this account
                        List<String> archives = archiveUrlsMap.getOrDefault(customer.getAccountNumber(), List.of());
                        if (archives.isEmpty()) {
                            // No archive, just add delivery file entry
                            SummaryProcessedFile entry = new SummaryProcessedFile();
                            BeanUtils.copyProperties(customer, entry);
                            switch (folder) {
                                case AppConstants.FOLDER_EMAIL -> entry.setPdfEmailFileUrl(blobUrl);
                                case AppConstants.FOLDER_MOBSTAT -> entry.setPdfMobstatFileUrl(blobUrl);
                            }
                            finalList.add(entry);
                        } else {
                            // Add entry per archive combo
                            for (String archiveUrl : archives) {
                                SummaryProcessedFile comboEntry = new SummaryProcessedFile();
                                BeanUtils.copyProperties(customer, comboEntry);
                                comboEntry.setArchiveBlobUrl(archiveUrl);
                                switch (folder) {
                                    case AppConstants.FOLDER_EMAIL -> comboEntry.setPdfEmailFileUrl(blobUrl);
                                    case AppConstants.FOLDER_MOBSTAT -> comboEntry.setPdfMobstatFileUrl(blobUrl);
                                }
                                finalList.add(comboEntry);
                            }
                        }
                        count++;
                    }
                }
            }
        }
        folderCounts.put(folder, count);
        logger.info("[{}] ✅ Uploaded {} {} files", msg.getBatchId(), count, folder);
    }

    // Step 3: Find all .ps files in jobDir and add to summary
    List<String> psFiles = new ArrayList<>();
    try (Stream<Path> stream = Files.walk(jobDir)) {
        for (Path file : stream.filter(Files::isRegularFile)
                               .filter(f -> f.getFileName().toString().endsWith(".ps"))
                               .filter(f -> !isTempFile(f))
                               .toList()) {
            psFiles.add(file.toAbsolutePath().toString());
        }
    }
    logger.info("[{}] ✅ Found {} .ps files in jobDir", msg.getBatchId(), psFiles.size());

    logger.info("[{}] ✅ Total processed files: {}", msg.getBatchId(), finalList.size());
    return finalList;
}

// Helper: Skip temp/hidden files
private boolean isTempFile(Path file) {
    String name = file.getFileName().toString().toLowerCase();
    return name.startsWith("~") || name.endsWith(".tmp") || name.endsWith(".temp") || name.equals(".ds_store");
}
