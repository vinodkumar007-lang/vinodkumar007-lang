private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles) {

    Map<String, ProcessedFileEntry> accountToEntry = new LinkedHashMap<>(); // preserve order

    for (SummaryProcessedFile file : processedFiles) {
        if (file == null || file.getAccountNumber() == null || file.getAccountNumber().isBlank()) continue;

        String key = file.getCustomerId() + "_" + file.getAccountNumber();
        ProcessedFileEntry entry = accountToEntry.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        // Map URLs — if already exists, keep existing (first upload) or override if new
        if (isNonEmpty(file.getPdfEmailFileUrl())) entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        if (isNonEmpty(file.getPdfMobstatFileUrl())) entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        if (isNonEmpty(file.getPrintFileUrl())) entry.setPrintBlobUrl(file.getPrintFileUrl());
        if (isNonEmpty(file.getArchiveBlobUrl())) entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

        // Determine delivery status using blob URLs and error map
        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

        entry.setEmailStatus(isNonEmpty(entry.getEmailBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
        entry.setMobstatStatus(isNonEmpty(entry.getMobstatBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(entry.getPrintBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(entry.getArchiveBlobUrl()) ? "SUCCESS" :
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

        accountToEntry.put(key, entry);
    }

    return new ArrayList<>(accountToEntry.values());
}
=========
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

    // 2️⃣ Merge customers by account to avoid duplicates
    Map<String, SummaryProcessedFile> accountToMergedEntry = new HashMap<>();
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) continue;
        String account = customer.getAccountNumber();
        if (account == null || account.isBlank()) continue;

        SummaryProcessedFile mergedEntry = accountToMergedEntry.getOrDefault(account, new SummaryProcessedFile());
        BeanUtils.copyProperties(customer, mergedEntry);

        // Attach archive URL if exists
        if (accountToArchiveUrl.containsKey(account)) {
            mergedEntry.setArchiveBlobUrl(accountToArchiveUrl.get(account));
        }

        accountToMergedEntry.put(account, mergedEntry);
    }

    // 3️⃣ Upload delivery files and attach URLs to merged entries
    for (String folder : deliveryFolders) {
        Path methodPath = jobDir.resolve(folder);
        if (!Files.exists(methodPath)) continue;

        String outputMethod = folderToOutputMethod.get(folder);
        try (Stream<Path> stream = Files.walk(methodPath)) {
            List<Path> deliveryFiles = stream.filter(Files::isRegularFile).toList();

            for (Path deliveryFile : deliveryFiles) {
                String fileName = deliveryFile.getFileName().toString();
                String account = extractAccountFromFileName(fileName);
                if (account == null || !accountToMergedEntry.containsKey(account)) continue;

                SummaryProcessedFile mergedEntry = accountToMergedEntry.get(account);
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
            logger.error("[{}] ⚠️ Failed to scan folder {}: {}", msg.getBatchId(), folder, e.getMessage(), e);
        }
    }

    // 4️⃣ Add all merged entries to final list
    finalList.addAll(accountToMergedEntry.values());

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}
