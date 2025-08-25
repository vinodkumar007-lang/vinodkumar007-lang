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

        // 1️⃣ Upload all archive files once and map by account
        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
        Map<String, String> accountToArchiveUrl = new HashMap<>();
        if (Files.exists(archivePath)) {
            try (Stream<Path> stream = Files.walk(archivePath)) {
                List<Path> archiveFiles = stream.filter(Files::isRegularFile).toList();
                for (Path archiveFile : archiveFiles) {
                    String fileName = archiveFile.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
                    if (account == null) continue;

                    try {
                        String archiveBlobUrl = blobStorageService.uploadFileByMessage(
                                archiveFile.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                        archiveBlobUrl = decodeUrl(archiveBlobUrl);
                        accountToArchiveUrl.put(account, archiveBlobUrl);
                        logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveBlobUrl);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed scanning ARCHIVE folder: {}", msg.getBatchId(), e.getMessage(), e);
            }
        }

        // 2️⃣ Process each customer and merge delivery files
        for (SummaryProcessedFile customer : customerList) {
            if (customer == null) continue;
            String account = customer.getAccountNumber();
            if (account == null || account.isBlank()) continue;

            SummaryProcessedFile mergedEntry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, mergedEntry);

            // Attach archive URL if exists
            if (accountToArchiveUrl.containsKey(account)) {
                mergedEntry.setArchiveBlobUrl(accountToArchiveUrl.get(account));
            }

            // Upload delivery files
            for (String folder : deliveryFolders) {
                Path methodPath = jobDir.resolve(folder);
                if (!Files.exists(methodPath)) continue;

                String outputMethod = folderToOutputMethod.get(folder);
                try (Stream<Path> stream = Files.walk(methodPath)) {
                    List<Path> deliveryFiles = stream
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .toList();

                    for (Path deliveryFile : deliveryFiles) {
                        try {
                            String deliveryBlobUrl = blobStorageService.uploadFileByMessage(deliveryFile.toFile(), folder, msg);
                            deliveryBlobUrl = decodeUrl(deliveryBlobUrl);

                            switch (folder) {
                                case AppConstants.FOLDER_EMAIL -> mergedEntry.setPdfEmailFileUrl(deliveryBlobUrl);
                                case AppConstants.FOLDER_MOBSTAT -> mergedEntry.setPdfMobstatFileUrl(deliveryBlobUrl);
                                case AppConstants.FOLDER_PRINT -> mergedEntry.setPrintFileUrl(deliveryBlobUrl);
                            }

                            logger.info("[{}] ✅ Uploaded {} file for account {}: {}", msg.getBatchId(), outputMethod, account, deliveryBlobUrl);
                        } catch (Exception e) {
                            logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}", msg.getBatchId(), outputMethod, account, e.getMessage(), e);
                        }
                    }
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to scan folder {} for account {}: {}", msg.getBatchId(), folder, account, e.getMessage(), e);
                }
            }

            // Add merged entry
            finalList.add(mergedEntry);
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }
    =====================
    
    // ===============================
// 1️⃣ Build detailed processed files per customer
// ===============================
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

    // -------- Upload all archive files once and map by account --------
    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    Map<String, String> accountToArchiveUrl = new HashMap<>();
    if (Files.exists(archivePath)) {
        try (Stream<Path> stream = Files.walk(archivePath)) {
            List<Path> archiveFiles = stream.filter(Files::isRegularFile).toList();
            for (Path archiveFile : archiveFiles) {
                String fileName = archiveFile.getFileName().toString();
                String account = extractAccountFromFileName(fileName); // implement accordingly
                if (account == null) continue;

                try {
                    String archiveBlobUrl = blobStorageService.uploadFileByMessage(
                            archiveFile.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                    archiveBlobUrl = decodeUrl(archiveBlobUrl);
                    accountToArchiveUrl.put(account, archiveBlobUrl);

                    logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveBlobUrl);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed scanning ARCHIVE folder: {}", msg.getBatchId(), e.getMessage(), e);
        }
    }

    // -------- Process each customer and merge delivery files --------
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) continue;
        String account = customer.getAccountNumber();
        if (account == null || account.isBlank()) continue;

        // Create merged entry per account
        SummaryProcessedFile mergedEntry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, mergedEntry);

        // Attach archive URL if exists
        if (accountToArchiveUrl.containsKey(account)) {
            mergedEntry.setArchiveBlobUrl(accountToArchiveUrl.get(account));
        }

        // Upload delivery files and merge into single entry
        for (String folder : deliveryFolders) {
            Path methodPath = jobDir.resolve(folder);
            if (!Files.exists(methodPath)) continue;

            String outputMethod = folderToOutputMethod.get(folder);
            try (Stream<Path> stream = Files.walk(methodPath)) {
                List<Path> deliveryFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .toList();

                for (Path deliveryFile : deliveryFiles) {
                    try {
                        String deliveryBlobUrl = blobStorageService.uploadFileByMessage(deliveryFile.toFile(), folder, msg);
                        deliveryBlobUrl = decodeUrl(deliveryBlobUrl);

                        switch (folder) {
                            case AppConstants.FOLDER_EMAIL -> mergedEntry.setPdfEmailFileUrl(deliveryBlobUrl);
                            case AppConstants.FOLDER_MOBSTAT -> mergedEntry.setPdfMobstatFileUrl(deliveryBlobUrl);
                            case AppConstants.FOLDER_PRINT -> mergedEntry.setPrintFileUrl(deliveryBlobUrl);
                        }

                        logger.info("[{}] ✅ Uploaded {} file for account {}: {}", msg.getBatchId(), outputMethod, account, deliveryBlobUrl);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}", msg.getBatchId(), outputMethod, account, e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to scan folder {} for account {}: {}", msg.getBatchId(), folder, account, e.getMessage(), e);
            }
        }

        // Add the single merged entry per customer/account
        finalList.add(mergedEntry);
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

// ===============================
// 2️⃣ Build processed file entries for summary.json
// ===============================
private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles) {

    List<ProcessedFileEntry> allEntries = new ArrayList<>();

    for (SummaryProcessedFile file : processedFiles) {
        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        // Map all URLs
        entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

        // Determine delivery status using blob URLs and error map
        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

        entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
        entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

        // Overall status logic
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

        // Mark PARTIAL if errors exist in errorMap
        if (errorMap.containsKey(entry.getAccountNumber()) && !"FAILED".equals(entry.getOverallStatus())) {
            entry.setOverallStatus("PARTIAL");
        }

        allEntries.add(entry);
    }

    return allEntries;
}
