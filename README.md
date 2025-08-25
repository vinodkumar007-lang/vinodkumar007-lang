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

        // -------- ARCHIVE upload (once per customer) --------
        List<String> archiveBlobUrls = new ArrayList<>();
        if (Files.exists(archivePath)) {
            try (Stream<Path> stream = Files.walk(archivePath)) {
                List<Path> archiveFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .toList();

                for (Path archiveFile : archiveFiles) {
                    try {
                        String archiveBlobUrl = blobStorageService.uploadFileByMessage(
                                archiveFile.toFile(), AppConstants.FOLDER_ARCHIVE, msg);

                        archiveBlobUrls.add(decodeUrl(archiveBlobUrl));

                        // Add pure archive entry
                        SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, archiveEntry);
                        archiveEntry.setOutputType(AppConstants.OUTPUT_ARCHIVE);
                        archiveEntry.setFileName(archiveFile.getFileName().toString());
                        archiveEntry.setBlobUrl(decodeUrl(archiveBlobUrl));
                        finalList.add(archiveEntry);

                        logger.info("[{}] 📦 Uploaded archive file for account {}: {}",
                                msg.getBatchId(), account, archiveBlobUrl);

                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload archive file for account {}: {}",
                                msg.getBatchId(), account, e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed scanning ARCHIVE folder for account {}: {}",
                        msg.getBatchId(), account, e.getMessage(), e);
            }
        }

        // -------- DELIVERY uploads (EMAIL, MOBSTAT, PRINT) --------
        for (String folder : deliveryFolders) {
            Path methodPath = jobDir.resolve(folder);

            if (!Files.exists(methodPath)) {
                logger.debug("[{}] Folder '{}' does not exist at path {}. Skipping.",
                        msg.getBatchId(), folder, methodPath);
                continue;
            }

            String outputMethod = folderToOutputMethod.get(folder);

            try (Stream<Path> stream = Files.walk(methodPath)) {
                List<Path> deliveryFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .toList();

                for (Path deliveryFile : deliveryFiles) {
                    try {
                        String deliveryBlobUrl = blobStorageService.uploadFileByMessage(
                                deliveryFile.toFile(), folder, msg);

                        SummaryProcessedFile entry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, entry);
                        entry.setOutputType(outputMethod);
                        entry.setFileName(deliveryFile.getFileName().toString());
                        entry.setBlobUrl(decodeUrl(deliveryBlobUrl));

                        // Attach all archive URLs without re-uploading
                        entry.setArchiveBlobUrls(new ArrayList<>(archiveBlobUrls));
                        finalList.add(entry);

                        logger.info("[{}] ✅ Uploaded {} file for account {}: {}",
                                msg.getBatchId(), outputMethod, account, deliveryBlobUrl);

                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}",
                                msg.getBatchId(), outputMethod, account, e.getMessage(), e);
                    }
                }
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
