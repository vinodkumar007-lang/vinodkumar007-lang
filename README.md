/**
 * Main method to upload all folder files and build detailed summary.
 */
private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    // Validate inputs
    if (!validateInputs(jobDir, customerList, msg)) {
        return finalList;
    }

    // All folders to scan
    List<String> allFolders = List.of(
            AppConstants.FOLDER_ARCHIVE,
            AppConstants.FOLDER_EMAIL,
            AppConstants.FOLDER_MOBSTAT,
            AppConstants.FOLDER_PRINT
    );

    logger.info("[{}] Starting upload of all folder files...", msg.getBatchId());

    // Step 1: Upload all files and group by account
    Map<String, Map<String, List<String>>> accountToFolderFiles = uploadAllFolderFiles(jobDir, allFolders, msg, errorMap);

    // Step 2: Build summary list using uploaded files
    finalList = buildSummaryFromUploadedFiles(customerList, accountToFolderFiles, msg);

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final summary list size: {}", msg.getBatchId(), finalList.size());

    return finalList;
}

/**
 * Uploads all files from given folders and groups them by account.
 * Returns a map: account -> folder -> list of blob URLs
 */
private Map<String, Map<String, List<String>>> uploadAllFolderFiles(Path jobDir,
                                                                    List<String> folders,
                                                                    KafkaMessage msg,
                                                                    Map<String, Map<String, String>> errorMap) {
    Map<String, Map<String, List<String>>> accountToFolderFiles = new HashMap<>();

    for (String folder : folders) {
        Path folderPath = jobDir.resolve(folder);
        logger.info("[{}] Scanning folder: {}", msg.getBatchId(), folderPath.toAbsolutePath());

        if (!Files.exists(folderPath) || !Files.isDirectory(folderPath)) {
            logger.warn("[{}] ⚠️ Folder does not exist: {}", msg.getBatchId(), folderPath);
            continue;
        }

        try (Stream<Path> stream = Files.walk(folderPath)) { // recursive scan
            stream.filter(Files::isRegularFile)
                  .filter(f -> !f.getFileName().toString().endsWith(".tmp"))
                  .forEach(file -> {
                      String fileName = file.getFileName().toString();
                      String account = extractAccountFromFileName(fileName);
                      if (account == null) account = "UNKNOWN"; // fallback to UNKNOWN

                      try {
                          String uploadedUrl = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), folder, msg));

                          accountToFolderFiles
                                  .computeIfAbsent(account, k -> new HashMap<>())
                                  .computeIfAbsent(folder, k -> new ArrayList<>())
                                  .add(uploadedUrl);

                          logger.info("[{}] ✅ Uploaded file [{}] for account [{}] in folder [{}]: {}",
                                  msg.getBatchId(), fileName, account, folder, uploadedUrl);

                      } catch (Exception e) {
                          logger.error("[{}] ⚠️ Failed to upload file [{}] in folder [{}]: {}",
                                  msg.getBatchId(), fileName, folder, e.getMessage(), e);
                          errorMap.computeIfAbsent(account, k -> new HashMap<>())
                                  .put(fileName, folder + " upload failed: " + e.getMessage());
                      }
                  });
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Error scanning folder {}: {}", msg.getBatchId(), folderPath, e.getMessage(), e);
        }

        int totalFiles = accountToFolderFiles.values().stream()
                .mapToInt(m -> m.getOrDefault(folder, Collections.emptyList()).size()).sum();
        logger.info("[{}] Total files uploaded in folder {}: {}", msg.getBatchId(), folder, totalFiles);
    }

    return accountToFolderFiles;
}

/**
 * Builds the final summary list using all uploaded files grouped by account.
 */
private List<SummaryProcessedFile> buildSummaryFromUploadedFiles(
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, List<String>>> accountToFolderFiles,
        KafkaMessage msg) {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    Set<String> uniqueKeys = new HashSet<>();

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;

        String account = customer.getAccountNumber();
        Map<String, List<String>> folderFiles = accountToFolderFiles.getOrDefault(account, Collections.emptyMap());

        List<String> archiveFiles = folderFiles.getOrDefault(AppConstants.FOLDER_ARCHIVE, Collections.emptyList());
        List<String> emailFiles = folderFiles.getOrDefault(AppConstants.FOLDER_EMAIL, Collections.emptyList());
        List<String> mobFiles = folderFiles.getOrDefault(AppConstants.FOLDER_MOBSTAT, Collections.emptyList());
        List<String> printFiles = folderFiles.getOrDefault(AppConstants.FOLDER_PRINT, Collections.emptyList());

        int maxFiles = Math.max(archiveFiles.size(),
                        Math.max(emailFiles.size(),
                        Math.max(mobFiles.size(), printFiles.size())));
        maxFiles = Math.max(maxFiles, 1); // at least one entry

        for (int i = 0; i < maxFiles; i++) {
            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);

            entry.setArchiveBlobUrl(i < archiveFiles.size() ? archiveFiles.get(i) : null);
            entry.setPdfEmailFileUrl(i < emailFiles.size() ? emailFiles.get(i) : null);
            entry.setPdfMobstatFileUrl(i < mobFiles.size() ? mobFiles.get(i) : null);
            entry.setPrintFileUrl(i < printFiles.size() ? printFiles.get(i) : null);

            // Unique key to avoid duplicates
            String key = account + "|" + entry.getArchiveBlobUrl() + "|" + entry.getPdfEmailFileUrl()
                    + "|" + entry.getPdfMobstatFileUrl() + "|" + entry.getPrintFileUrl();
            if (!uniqueKeys.add(key)) continue;

            finalList.add(entry);
        }
    }

    logger.info("[{}] ✅ Final summary list size: {}", msg.getBatchId(), finalList.size());
    return finalList;
}

/**
 * Extracts account number from filename.
 * Looks for numeric sequences of 3–12 digits anywhere in filename.
 * Returns "UNKNOWN" if not found.
 */
private String extractAccountFromFileName(String fileName) {
    if (fileName == null || fileName.isBlank()) return "UNKNOWN";

    String normalized = fileName.trim();
    Pattern pattern = Pattern.compile("\\b\\d{3,12}\\b");
    Matcher matcher = pattern.matcher(normalized);

    if (matcher.find()) {
        String account = matcher.group();
        logger.debug("Extracted account [{}] from filename [{}]", account, fileName);
        return account;
    }

    logger.warn("Could not extract account from filename: {}. Assigning UNKNOWN.", fileName);
    return "UNKNOWN";
}

/**
 * Validate inputs for buildDetailedProcessedFiles
 */
private boolean validateInputs(Path jobDir, List<SummaryProcessedFile> customerList, KafkaMessage msg) {
    if (jobDir == null) {
        logger.warn("[{}] ⚠️ jobDir is null. Skipping buildDetailedProcessedFiles.", msg != null ? msg.getBatchId() : "UNKNOWN");
        return false;
    }
    if (customerList == null || customerList.isEmpty()) {
        logger.warn("[{}] ⚠️ customerList is null/empty. Nothing to process.", msg != null ? msg.getBatchId() : "UNKNOWN");
        return false;
    }
    if (msg == null) {
        logger.warn("[UNKNOWN] ⚠️ KafkaMessage is null. Skipping buildDetailedProcessedFiles.");
        return false;
    }
    return true;
}
