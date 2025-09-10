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

    // Counters for logging
    Map<String, Integer> uploadCounts = new HashMap<>();
    deliveryFolders.forEach(f -> uploadCounts.put(f, 0));

    // Step 1: Upload all archive files and group by account
    Map<String, List<String>> accountToArchiveUrls = new HashMap<>();
    Path archiveFolder = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    if (Files.exists(archiveFolder)) {
        try (Stream<Path> stream = Files.walk(archiveFolder)) {
            stream.filter(Files::isRegularFile)
                  .filter(f -> !isTempFile(f))
                  .forEach(f -> {
                      String fileName = f.getFileName().toString();
                      customerList.forEach(cust -> {
                          if (fileName.contains(cust.getAccountNumber())) {
                              try {
                                  String blobUrl = blobStorageService.uploadFileByMessage(f.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                                  accountToArchiveUrls.computeIfAbsent(cust.getAccountNumber(), k -> new ArrayList<>()).add(blobUrl);
                                  uploadCounts.put(AppConstants.FOLDER_ARCHIVE, uploadCounts.get(AppConstants.FOLDER_ARCHIVE) + 1);

                                  // Add archive-only entry
                                  SummaryProcessedFile entry = new SummaryProcessedFile();
                                  BeanUtils.copyProperties(cust, entry);
                                  entry.setArchiveBlobUrl(blobUrl);
                                  finalList.add(entry);

                              } catch (Exception e) {
                                  logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage());
                              }
                          }
                      });
                  });
        }
    }

    // Step 2: Process other delivery folders
    for (String folder : deliveryFolders) {
        if (folder.equals(AppConstants.FOLDER_ARCHIVE)) continue; // Already processed

        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) {
            logger.warn("[{}] ⚠️ Folder not found: {}", msg.getBatchId(), folderPath);
            continue;
        }

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                  .filter(f -> !isTempFile(f))
                  .forEach(file -> {
                      String fileName = file.getFileName().toString();
                      customerList.forEach(customerEntry -> {
                          boolean matchesAccount = fileName.contains(customerEntry.getAccountNumber()) || fileName.endsWith(".ps");
                          if (matchesAccount) {
                              try {
                                  // Upload delivery file
                                  String deliveryBlobUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);
                                  uploadCounts.put(folder, uploadCounts.get(folder) + 1);
                                  logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folder, deliveryBlobUrl);

                                  // Combine with all archive files for the same account
                                  List<String> archives = accountToArchiveUrls.getOrDefault(customerEntry.getAccountNumber(), Collections.emptyList());
                                  if (archives.isEmpty()) {
                                      // No archive, still add delivery only
                                      SummaryProcessedFile entry = new SummaryProcessedFile();
                                      BeanUtils.copyProperties(customerEntry, entry);
                                      setDeliveryFileUrl(entry, folder, deliveryBlobUrl);
                                      finalList.add(entry);
                                  } else {
                                      for (String archiveUrl : archives) {
                                          SummaryProcessedFile comboEntry = new SummaryProcessedFile();
                                          BeanUtils.copyProperties(customerEntry, comboEntry);
                                          comboEntry.setArchiveBlobUrl(archiveUrl);
                                          setDeliveryFileUrl(comboEntry, folder, deliveryBlobUrl);
                                          finalList.add(comboEntry);
                                      }
                                  }
                              } catch (Exception e) {
                                  logger.error("[{}] ⚠️ Failed to upload delivery file {}: {}", msg.getBatchId(), fileName, e.getMessage());
                              }
                          }
                      });
                  });
        }
    }

    // Log summary of uploaded files
    uploadCounts.forEach((folder, count) -> logger.info("[{}] 📂 Total {} files uploaded: {}", msg.getBatchId(), folder, count));

    logger.info("[{}] ✅ Total processed entries in summary.json: {}", msg.getBatchId(), finalList.size());
    return finalList;
}

// Helper: Set the delivery URL based on folder type
private void setDeliveryFileUrl(SummaryProcessedFile entry, String folder, String url) {
    switch (folder) {
        case AppConstants.FOLDER_EMAIL -> entry.setPdfEmailFileUrl(url);
        case AppConstants.FOLDER_MOBSTAT -> entry.setPdfMobstatFileUrl(url);
        case AppConstants.FOLDER_PRINT -> entry.setPrintFileUrl(url);
    }
}

// Helper: Skip temp/hidden files
private boolean isTempFile(Path file) {
    String name = file.getFileName().toString().toLowerCase();
    return name.startsWith("~") || name.endsWith(".tmp") || name.endsWith(".temp") || name.equals(".ds_store");
}
