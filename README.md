private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    List<String> deliveryFolders = List.of(
            AppConstants.FOLDER_EMAIL,
            AppConstants.FOLDER_MOBSTAT,
            AppConstants.FOLDER_PRINT
    );

    Map<String, String> folderToOutputMethod = Map.of(
            AppConstants.FOLDER_EMAIL, AppConstants.OUTPUT_EMAIL,
            AppConstants.FOLDER_MOBSTAT, AppConstants.OUTPUT_MOBSTAT,
            AppConstants.FOLDER_PRINT, AppConstants.OUTPUT_PRINT
    );

    logger.info("[{}] 🔍 Entered buildDetailedProcessedFiles with jobDir={}, customerList size={}",
            msg.getBatchId(), jobDir, (customerList != null ? customerList.size() : null));

    if (jobDir == null || customerList == null || msg == null) {
        logger.warn("[{}] ⚠️ One or more input parameters are null: jobDir={}, customerList={}, msg={}",
                (msg != null ? msg.getBatchId() : "N/A"), jobDir, customerList, msg);
        return finalList;
    }

    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    logger.debug("[{}] 📂 Archive folder path resolved to: {}", msg.getBatchId(), archivePath);

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) {
            logger.debug("[{}] ⚠️ Skipping null customer entry", msg.getBatchId());
            continue;
        }

        String account = customer.getAccountNumber();
        logger.info("[{}] ➡️ Processing customer with accountNumber={}", msg.getBatchId(), account);

        if (account == null || account.isBlank()) {
            logger.warn("[{}] ⚠️ Skipping customer with empty account number", msg.getBatchId());
            continue;
        }

        // -------- ARCHIVE upload --------
        String archiveBlobUrl = null;
        if (Files.exists(archivePath)) {
            try (Stream<Path> stream = Files.walk(archivePath)) {
                Optional<Path> archiveFile = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (archiveFile.isPresent()) {
                    archiveBlobUrl = blobStorageService.uploadFileByMessage(
                            archiveFile.get().toFile(), AppConstants.FOLDER_ARCHIVE, msg);

                    SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, archiveEntry);
                    archiveEntry.setOutputType(AppConstants.OUTPUT_ARCHIVE);
                    archiveEntry.setBlobUrl(decodeUrl(archiveBlobUrl));
                    finalList.add(archiveEntry);
                    logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveBlobUrl);
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to upload archive file for account {}: {}", msg.getBatchId(), account, e.getMessage(), e);
            }
        }

        // -------- EMAIL, MOBSTAT, PRINT uploads --------
        for (String folder : deliveryFolders) {
            Path methodPath = jobDir.resolve(folder);

            // Skip if folder doesn't exist
            if (!Files.exists(methodPath)) {
                logger.debug("[{}] Folder '{}' does not exist at path {}. Skipping.", msg.getBatchId(), folder, methodPath);
                continue;
            }

            String outputMethod = folderToOutputMethod.get(folder);

            try (Stream<Path> stream = Files.walk(methodPath)) {
                stream.filter(Files::isRegularFile)
                      .filter(p -> p.getFileName().toString().contains(account))
                      .forEach(p -> {
                          try {
                              String blobUrl = blobStorageService.uploadFileByMessage(p.toFile(), folder, msg);
                              logger.info("[{}] ✅ Uploaded {} file for account {}: {}", msg.getBatchId(), outputMethod, account, blobUrl);

                              SummaryProcessedFile entry = new SummaryProcessedFile();
                              BeanUtils.copyProperties(customer, entry);
                              entry.setOutputType(outputMethod);
                              entry.setBlobUrl(decodeUrl(blobUrl));

                              if (archiveBlobUrl != null) {
                                  entry.setArchiveOutputType(AppConstants.OUTPUT_ARCHIVE);
                                  entry.setArchiveBlobUrl(archiveBlobUrl);
                              }

                              finalList.add(entry);
                          } catch (Exception e) {
                              logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}", msg.getBatchId(), outputMethod, account, e.getMessage(), e);
                          }
                      });
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to scan folder {} for account {}: {}", msg.getBatchId(), folder, account, e.getMessage(), e);
            }
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}
