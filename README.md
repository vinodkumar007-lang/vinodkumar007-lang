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

    // Load all archive files first for combination
    Map<String, List<Path>> archiveFilesMap = new HashMap<>();
    Path archiveFolder = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    if (Files.exists(archiveFolder)) {
        try (Stream<Path> stream = Files.walk(archiveFolder)) {
            stream.filter(Files::isRegularFile)
                  .filter(f -> !isTempFile(f))
                  .forEach(f -> {
                      String fileName = f.getFileName().toString();
                      customerList.forEach(cust -> {
                          if (fileName.contains(cust.getAccountNumber())) {
                              archiveFilesMap.computeIfAbsent(cust.getAccountNumber(), k -> new ArrayList<>()).add(f);
                          }
                      });
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
            stream.filter(Files::isRegularFile)
                  .filter(f -> !isTempFile(f))
                  .forEach(file -> {
                      try {
                          String fileName = file.getFileName().toString();

                          // Upload delivery file to blob
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

                                  // Add archive + delivery combinations if folder != archive
                                  if (!folder.equals(AppConstants.FOLDER_ARCHIVE)) {
                                      List<Path> archives = archiveFilesMap.getOrDefault(customerEntry.getAccountNumber(), Collections.emptyList());
                                      for (Path archiveFile : archives) {
                                          try {
                                              String archiveBlobUrl = blobStorageService.uploadFileByMessage(archiveFile.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                                              SummaryProcessedFile comboEntry = new SummaryProcessedFile();
                                              BeanUtils.copyProperties(customerEntry, comboEntry);

                                              // Set delivery file
                                              switch (folder) {
                                                  case AppConstants.FOLDER_EMAIL -> comboEntry.setPdfEmailFileUrl(blobUrl);
                                                  case AppConstants.FOLDER_MOBSTAT -> comboEntry.setPdfMobstatFileUrl(blobUrl);
                                                  case AppConstants.FOLDER_PRINT -> comboEntry.setPrintFileUrl(blobUrl);
                                              }
                                              comboEntry.setArchiveBlobUrl(archiveBlobUrl);
                                              finalList.add(comboEntry);
                                          } catch (Exception e) {
                                              logger.error("[{}] ⚠️ Failed archive combo upload: {}", msg.getBatchId(), e.getMessage());
                                          }
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
