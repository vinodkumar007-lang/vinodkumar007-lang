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

    // -------- ARCHIVE upload (all archive files, not filtered by account) --------
    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    logger.debug("[{}] 📂 Archive folder path resolved to: {}", msg.getBatchId(), archivePath);

    List<String> archiveBlobUrls = new ArrayList<>();
    if (Files.exists(archivePath)) {
        try (Stream<Path> stream = Files.walk(archivePath)) {
            List<Path> archiveFiles = stream
                    .filter(Files::isRegularFile)
                    .toList();   // ✅ no account filter

            for (Path archiveFile : archiveFiles) {
                try {
                    String archiveBlobUrl = blobStorageService.uploadFileByMessage(
                            archiveFile.toFile(), AppConstants.FOLDER_ARCHIVE, msg);

                    String decodedUrl = decodeUrl(archiveBlobUrl);
                    archiveBlobUrls.add(decodedUrl);

                    // add a pure archive entry
                    SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                    archiveEntry.setOutputType(AppConstants.OUTPUT_ARCHIVE);
                    archiveEntry.setFileName(archiveFile.getFileName().toString());
                    archiveEntry.setBlobUrl(decodedUrl);
                    finalList.add(archiveEntry);

                    logger.info("[{}] 📦 Uploaded archive file: {}", msg.getBatchId(), decodedUrl);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload archive file {}: {}",
                            msg.getBatchId(), archiveFile, e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed scanning ARCHIVE folder: {}", msg.getBatchId(), e.getMessage(), e);
        }
    }

    // -------- Process each customer (EMAIL / MOBSTAT / PRINT uploads) --------
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

        for (String folder : deliveryFolders) {
            Path methodPath = jobDir.resolve(folder);

            if (!Files.exists(methodPath)) {
                logger.debug("[{}] Folder '{}' does not exist at path {}. Skipping.",
                        msg.getBatchId(), folder, methodPath);
                continue;
            }

            String outputMethod = folderToOutputMethod.get(folder);

            try (Stream<Path> stream = Files.walk(methodPath)) {
                stream.filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account)) // still filter delivery files by account
                        .forEach(p -> {
                            try {
                                String blobUrl = blobStorageService.uploadFileByMessage(p.toFile(), folder, msg);
                                String decodedUrl = decodeUrl(blobUrl);

                                logger.info("[{}] ✅ Uploaded {} file for account {}: {}",
                                        msg.getBatchId(), outputMethod, account, decodedUrl);

                                if (!archiveBlobUrls.isEmpty()) {
                                    // pair this delivery file with all archive files
                                    for (String archiveBlobUrl : archiveBlobUrls) {
                                        SummaryProcessedFile entry = new SummaryProcessedFile();
                                        BeanUtils.copyProperties(customer, entry);
                                        entry.setOutputType(outputMethod);
                                        entry.setFileName(p.getFileName().toString());
                                        entry.setBlobUrl(decodedUrl);
                                        entry.setArchiveOutputType(AppConstants.OUTPUT_ARCHIVE);
                                        entry.setArchiveBlobUrl(archiveBlobUrl);

                                        finalList.add(entry);
                                    }
                                } else {
                                    // fallback: no archives uploaded
                                    SummaryProcessedFile entry = new SummaryProcessedFile();
                                    BeanUtils.copyProperties(customer, entry);
                                    entry.setOutputType(outputMethod);
                                    entry.setFileName(p.getFileName().toString());
                                    entry.setBlobUrl(decodedUrl);
                                    finalList.add(entry);
                                }
                            } catch (Exception e) {
                                logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}",
                                        msg.getBatchId(), outputMethod, account, e.getMessage(), e);
                            }
                        });
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to scan folder {} for account {}: {}",
                        msg.getBatchId(), folder, account, e.getMessage(), e);
            }
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}",
            msg.getBatchId(), finalList.size());
    return finalList;
}
