private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    if (customerList == null || customerList.isEmpty() || jobDir == null || !Files.exists(jobDir)) {
        logger.warn("[{}] ⚠️ Job directory or customer list empty", msg.getBatchId());
        return finalList;
    }

    // All delivery folders
    List<String> deliveryFolders = List.of(
            AppConstants.FOLDER_ARCHIVE,
            AppConstants.FOLDER_EMAIL,
            AppConstants.FOLDER_MOBSTAT,
            AppConstants.FOLDER_PRINT
    );

    // Upload files from all folders
    Map<String, Map<String, String>> uploadedFilesMap = new HashMap<>();
    for (String folder : deliveryFolders) {
        Path folderPath = jobDir.resolve(folder);
        Map<String, String> fileUrlMap = new HashMap<>();

        if (Files.exists(folderPath)) {
            try (Stream<Path> stream = Files.walk(folderPath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    try {
                        String fileName = file.getFileName().toString();
                        String blobUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);
                        fileUrlMap.put(fileName, blobUrl);
                        logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folder, blobUrl);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(), folder, file.getFileName(), e.getMessage());
                    }
                });
            }
        } else {
            logger.warn("[{}] ⚠️ Folder not found: {}", msg.getBatchId(), folderPath);
        }
        uploadedFilesMap.put(folder, fileUrlMap);
    }

    // Build summary processed files for each customer
    for (SummaryProcessedFile entry : customerList) {
        String account = entry.getAccountNumber();

        // Archive files
        entry.setArchiveFileUrls(findFilesForAccount(uploadedFilesMap.get(AppConstants.FOLDER_ARCHIVE), account));
        // Email files
        entry.setPdfEmailFileUrl(findFirstFileForAccount(uploadedFilesMap.get(AppConstants.FOLDER_EMAIL), account));
        // Mobstat files (include mobstat + archive combos)
        entry.setPdfMobstatFileUrl(findFirstFileForAccount(uploadedFilesMap.get(AppConstants.FOLDER_MOBSTAT), account));
        // Print files
        entry.setPrintFileUrl(findFirstFileForAccount(uploadedFilesMap.get(AppConstants.FOLDER_PRINT), account));

        finalList.add(entry);
    }

    return finalList;
}

// Helper: Return all matching files for an account
private List<String> findFilesForAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null) return Collections.emptyList();
    return fileMap.entrySet().stream()
            .filter(e -> e.getKey().contains(account))
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
}

// Helper: Return first matching file for an account
private String findFirstFileForAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null) return null;
    return fileMap.entrySet().stream()
            .filter(e -> e.getKey().contains(account))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
}
