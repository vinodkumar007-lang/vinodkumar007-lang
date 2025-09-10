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

    // ----------------------------
    // Step 1: Upload all archive files first and map per account + filename
    // ----------------------------
    Map<String, List<String>> archiveUrlsMap = new HashMap<>(); // account -> list of archive URLs
    Map<String, List<String>> archiveFileNameMap = new HashMap<>(); // account -> list of archive filenames
    Path archiveFolder = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    int archiveCount = 0;

    if (Files.exists(archiveFolder)) {
        try (Stream<Path> stream = Files.walk(archiveFolder)) {
            List<Path> archiveFiles = stream.filter(Files::isRegularFile)
                                            .filter(f -> !isTempFile(f))
                                            .toList();
            for (Path file : archiveFiles) {
                String fileName = file.getFileName().toString();
                String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                archiveCount++;

                // Map archive URLs per account + filename
                for (SummaryProcessedFile cust : customerList) {
                    if (fileName.contains(cust.getAccountNumber())) {
                        archiveUrlsMap.computeIfAbsent(cust.getAccountNumber(), k -> new ArrayList<>()).add(blobUrl);
                        archiveFileNameMap.computeIfAbsent(cust.getAccountNumber(), k -> new ArrayList<>()).add(fileName);
                    }
                }
            }
        }
    }
    logger.info("[{}] ✅ Total archive files uploaded: {}", msg.getBatchId(), archiveCount);

    // ----------------------------
    // Step 2: Process delivery folders (email/mobstat)
    // ----------------------------
    for (String folder : deliveryFolders) {
        if (folder.equals(AppConstants.FOLDER_ARCHIVE)) continue;

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
                String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);
                folderCount++;

                // Match customer: filename first, fallback to account number
                for (SummaryProcessedFile cust : customerList) {
                    boolean matched = archiveFileNameMap.getOrDefault(cust.getAccountNumber(), Collections.emptyList())
                                                        .stream()
                                                        .anyMatch(fnm -> fileName.equalsIgnoreCase(fnm));

                    if (!matched && fileName.contains(cust.getAccountNumber())) {
                        matched = true;
                    }

                    if (matched) {
                        // Create combined entry for each archive URL
                        List<String> archives = archiveUrlsMap.getOrDefault(cust.getAccountNumber(), Collections.emptyList());
                        if (archives.isEmpty()) {
                            // No archive found, just delivery file
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
                }
            }
        }
        logger.info("[{}] ✅ Total {} files uploaded: {}", msg.getBatchId(), folder, folderCount);
    }

    // ----------------------------
    // Step 3: Collect all .ps files from jobDir for summary
    // ----------------------------
    List<String> psFiles = new ArrayList<>();
    try (Stream<Path> stream = Files.walk(jobDir)) {
        psFiles = stream.filter(Files::isRegularFile)
                        .filter(f -> f.getFileName().toString().toLowerCase().endsWith(".ps"))
                        .map(Path::toString)
                        .collect(Collectors.toList());
    }
    logger.info("[{}] ✅ Total .ps files found: {}", msg.getBatchId(), psFiles.size());

    // ----------------------------
    // Step 4: Log final summary
    // ----------------------------
    logger.info("[{}] ✅ Total processed entries in final list: {}", msg.getBatchId(), finalList.size());

    return finalList;
}

// ----------------------------
// Helper: Set delivery URL
// ----------------------------
private void setDeliveryUrl(SummaryProcessedFile entry, String folder, String url) {
    switch (folder) {
        case AppConstants.FOLDER_EMAIL -> entry.setPdfEmailFileUrl(url);
        case AppConstants.FOLDER_MOBSTAT -> entry.setPdfMobstatFileUrl(url);
        case AppConstants.FOLDER_PRINT -> entry.setPrintFileUrl(url);
    }
}

// ----------------------------
// Helper: Skip temp/hidden files
// ----------------------------
private boolean isTempFile(Path file) {
    String name = file.getFileName().toString().toLowerCase();
    return name.startsWith("~") || name.endsWith(".tmp") || name.endsWith(".temp") || name.equals(".ds_store");
}
