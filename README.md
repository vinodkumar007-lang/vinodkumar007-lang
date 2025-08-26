// ===============================
// 1️⃣ Build detailed processed files per customer (merged, no duplicates)
// ===============================
private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

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

    // -------- Upload all archive files once and map by account --------
    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    Map<String, String> accountToArchiveUrl = new HashMap<>();
    if (Files.exists(archivePath)) {
        try (Stream<Path> stream = Files.walk(archivePath)) {
            stream.filter(Files::isRegularFile).forEach(archiveFile -> {
                String fileName = archiveFile.getFileName().toString();
                String account = extractAccountFromFileName(fileName);
                if (account == null) return;

                try {
                    String archiveBlobUrl = decodeUrl(
                            blobStorageService.uploadFileByMessage(archiveFile.toFile(), AppConstants.FOLDER_ARCHIVE, msg)
                    );
                    accountToArchiveUrl.put(account, archiveBlobUrl);
                    logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveBlobUrl);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed scanning ARCHIVE folder: {}", msg.getBatchId(), e.getMessage(), e);
        }
    }

    // -------- Process each customer and merge delivery files --------
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) continue;
        String account = customer.getAccountNumber();
        if (account == null || account.isBlank()) continue;

        SummaryProcessedFile mergedEntry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, mergedEntry);

        // Attach archive URL
        if (accountToArchiveUrl.containsKey(account)) {
            mergedEntry.setArchiveBlobUrl(accountToArchiveUrl.get(account));
        }

        // Upload delivery files and merge
        for (String folder : deliveryFolders) {
            Path methodPath = jobDir.resolve(folder);
            if (!Files.exists(methodPath)) continue;

            try (Stream<Path> stream = Files.walk(methodPath)) {
                stream.filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .forEach(deliveryFile -> {
                            try {
                                String deliveryBlobUrl = decodeUrl(
                                        blobStorageService.uploadFileByMessage(deliveryFile.toFile(), folder, msg)
                                );
                                switch (folder) {
                                    case AppConstants.FOLDER_EMAIL -> mergedEntry.setPdfEmailFileUrl(deliveryBlobUrl);
                                    case AppConstants.FOLDER_MOBSTAT -> mergedEntry.setPdfMobstatFileUrl(deliveryBlobUrl);
                                    case AppConstants.FOLDER_PRINT -> mergedEntry.setPrintFileUrl(deliveryBlobUrl);
                                }
                                logger.info("[{}] ✅ Uploaded {} file for account {}: {}", msg.getBatchId(), folderToOutputMethod.get(folder), account, deliveryBlobUrl);
                            } catch (Exception e) {
                                logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}", msg.getBatchId(), folderToOutputMethod.get(folder), account, e.getMessage(), e);
                            }
                        });
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to scan folder {} for account {}: {}", msg.getBatchId(), folder, account, e.getMessage(), e);
            }
        }

        finalList.add(mergedEntry);
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

// ===============================
// 2️⃣ Build processed file entries for summary.json (duplicates removed)
// ===============================
private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles) {

    Map<String, ProcessedFileEntry> uniqueEntries = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "_" + file.getAccountNumber();
        ProcessedFileEntry entry = uniqueEntries.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        // Map all URLs (overwrite if multiple files merged)
        if (isNonEmpty(file.getPdfEmailFileUrl())) entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        if (isNonEmpty(file.getPdfMobstatFileUrl())) entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        if (isNonEmpty(file.getPrintFileUrl())) entry.setPrintBlobUrl(file.getPrintFileUrl());
        if (isNonEmpty(file.getArchiveBlobUrl())) entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

        // Determine delivery status
        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());
        entry.setEmailStatus(isNonEmpty(entry.getEmailBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
        entry.setMobstatStatus(isNonEmpty(entry.getMobstatBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(entry.getPrintBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(entry.getArchiveBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

        // Overall status
        boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
        boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
        boolean printSuccess = "SUCCESS".equals(entry.getPrintStatus());
        boolean archiveSuccess = "SUCCESS".equals(entry.getArchiveStatus());

        boolean emailFailed = !emailSuccess;
        boolean mobstatFailed = !mobstatSuccess;
        boolean printFailed = !printSuccess;

        if ((emailSuccess && archiveSuccess) ||
                (mobstatSuccess && archiveSuccess && emailFailed && printFailed) ||
                (printSuccess && archiveSuccess && emailFailed && mobstatFailed)) {
            entry.setOverallStatus("SUCCESS");
        } else if (archiveSuccess) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }

        // Mark PARTIAL if errorMap exists
        if (errorMap.containsKey(entry.getAccountNumber()) && !"FAILED".equals(entry.getOverallStatus())) {
            entry.setOverallStatus("PARTIAL");
        }

        uniqueEntries.put(key, entry);
    }

    return new ArrayList<>(uniqueEntries.values());
}
