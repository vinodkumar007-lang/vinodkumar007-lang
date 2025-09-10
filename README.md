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

    // Step 1: Upload all archive files first
    Map<String, List<String>> archiveUrlsMap = new HashMap<>(); // account -> list of archive URLs
    Path archiveFolder = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    int archiveCount = 0;

    if (Files.exists(archiveFolder)) {
        try (Stream<Path> stream = Files.walk(archiveFolder)) {
            for (Path file : stream.filter(Files::isRegularFile).filter(f -> !isTempFile(f)).toList()) {
                String fileName = file.getFileName().toString();
                String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                archiveCount++;

                // Map archive URLs per account
                customerList.forEach(cust -> {
                    if (fileName.contains(cust.getAccountNumber())) {
                        archiveUrlsMap.computeIfAbsent(cust.getAccountNumber(), k -> new ArrayList<>()).add(blobUrl);
                    }
                });
            }
        }
    }
    logger.info("[{}] ✅ Total archive files uploaded: {}", msg.getBatchId(), archiveCount);

    // Step 2: Process email/mobstat folders
    Map<String, Integer> folderUploadCount = new HashMap<>();
    for (String folder : deliveryFolders) {
        if (folder.equals(AppConstants.FOLDER_ARCHIVE)) continue; // already done

        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) {
            logger.warn("[{}] ⚠️ Folder not found: {}", msg.getBatchId(), folderPath);
            continue;
        }

        int folderCount = 0;
        try (Stream<Path> stream = Files.walk(folderPath)) {
            for (Path file : stream.filter(Files::isRegularFile).filter(f -> !isTempFile(f)).toList()) {
                String fileName = file.getFileName().toString();
                String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);
                folderCount++;

                // Match customer: filename first, fallback to account number
                customerList.forEach(cust -> {
                    boolean matched = fileName.contains(cust.getAccountNumber());
                    if (!matched) {
                        matched = true; // fallback: always associate by account number
                    }

                    if (matched) {
                        // For each archive URL for this account, create a combined entry
                        List<String> archives = archiveUrlsMap.getOrDefault(cust.getAccountNumber(), Collections.emptyList());
                        if (archives.isEmpty()) {
                            // If no archive, still create one entry with delivery only
                            SummaryProcessedFile entry = new SummaryProcessedFile();
                            BeanUtils.copyProperties(cust, entry);
                            setDeliveryUrl(entry, folder, blobUrl);
                            finalList.add(entry);
                        } else {
                            for (String archiveUrl : archives) {
                                SummaryProcessedFile entry = new SummaryProcessedFile();
                                BeanUtils.copyProperties(cust, entry);
                                entry.setArchiveBlobUrl(archiveUrl);
                                setDeliveryUrl(entry, folder, blobUrl);
                                finalList.add(entry);
                            }
                        }
                    }
                });
            }
        }
        folderUploadCount.put(folder, folderCount);
        logger.info("[{}] ✅ Total {} files uploaded: {}", msg.getBatchId(), folder, folderCount);
    }

    // Step 3: Collect all .ps files from jobDir for summary
    List<String> psFiles = new ArrayList<>();
    try (Stream<Path> stream = Files.walk(jobDir)) {
        psFiles = stream.filter(Files::isRegularFile)
                .filter(f -> f.getFileName().toString().toLowerCase().endsWith(".ps"))
                .map(Path::toString)
                .collect(Collectors.toList());
    }
    logger.info("[{}] ✅ Total .ps files found: {}", msg.getBatchId(), psFiles.size());

    logger.info("[{}] ✅ Total processed entries in final list: {}", msg.getBatchId(), finalList.size());
    return finalList;
}

// Helper: set delivery URL
private void setDeliveryUrl(SummaryProcessedFile entry, String folder, String url) {
    switch (folder) {
        case AppConstants.FOLDER_EMAIL -> entry.setPdfEmailFileUrl(url);
        case AppConstants.FOLDER_MOBSTAT -> entry.setPdfMobstatFileUrl(url);
        case AppConstants.FOLDER_PRINT -> entry.setPrintFileUrl(url);
    }
}

// Helper: skip temp/hidden files
private boolean isTempFile(Path file) {
    String name = file.getFileName().toString().toLowerCase();
    return name.startsWith("~") || name.endsWith(".tmp") || name.endsWith(".temp") || name.equals(".ds_store");
}
