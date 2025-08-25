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

    // -------- 1️⃣ Upload all archive files once and map by account --------
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

    // -------- 2️⃣ Process each customer and merge delivery types --------
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) continue;
        String account = customer.getAccountNumber();
        if (account == null || account.isBlank()) continue;

        // Start a single merged entry per customer/account
        SummaryProcessedFile mergedEntry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, mergedEntry);
        mergedEntry.setOutputType("MERGED");
        mergedEntry.setArchiveBlobUrl(accountToArchiveUrl.get(account));
        mergedEntry.setOverallStatus("SUCCESS"); // will adjust later based on actual delivery uploads

        // -------- 3️⃣ Upload delivery files --------
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

        finalList.add(mergedEntry);
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}
====

private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles) {

    List<ProcessedFileEntry> allEntries = new ArrayList<>();

    for (SummaryProcessedFile file : processedFiles) {
        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        // Assign delivery URLs from merged file
        entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

        // Determine status for each delivery type
        entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS"
                : errors.getOrDefault(AppConstants.OUTPUT_EMAIL, "").equalsIgnoreCase("FAILED") ? "FAILED" : "");
        entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS"
                : errors.getOrDefault(AppConstants.OUTPUT_MOBSTAT, "").equalsIgnoreCase("FAILED") ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS"
                : errors.getOrDefault(AppConstants.OUTPUT_PRINT, "").equalsIgnoreCase("FAILED") ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" : "FAILED");

        // Calculate overallStatus
        boolean anySuccess = "SUCCESS".equals(entry.getEmailStatus()) || "SUCCESS".equals(entry.getMobstatStatus())
                || "SUCCESS".equals(entry.getPrintStatus()) || "SUCCESS".equals(entry.getArchiveStatus());

        if (anySuccess) {
            entry.setOverallStatus("SUCCESS");
        } else if (!anySuccess && (entry.getArchiveStatus() != null && entry.getArchiveStatus().equals("SUCCESS"))) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }

        // Adjust based on errorMap
        if (errorMap.containsKey(entry.getAccountNumber()) && !"FAILED".equals(entry.getOverallStatus())) {
            entry.setOverallStatus("PARTIAL");
        }

        allEntries.add(entry);
    }

    return allEntries;
}
