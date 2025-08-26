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

    // -------- Upload archive files --------
    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    Map<String, Map<String, String>> accountFileToArchiveUrl = new HashMap<>();
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

                    accountFileToArchiveUrl.computeIfAbsent(account, k -> new HashMap<>())
                                           .put(fileName, archiveBlobUrl);

                    logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveBlobUrl);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed scanning ARCHIVE folder: {}", msg.getBatchId(), e.getMessage(), e);
        }
    }

    // -------- Upload delivery files (email/mobstat/print) --------
    Map<String, Map<String, String>> emailFiles = new HashMap<>();
    Map<String, Map<String, String>> mobstatFiles = new HashMap<>();
    Map<String, Map<String, String>> printFiles = new HashMap<>();

    for (String folder : deliveryFolders) {
        Path methodPath = jobDir.resolve(folder);
        if (!Files.exists(methodPath)) continue;

        try (Stream<Path> stream = Files.walk(methodPath)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                String fileName = file.getFileName().toString();
                String account = extractAccountFromFileName(fileName);
                if (account == null) return;

                try {
                    String deliveryBlobUrl = decodeUrl(
                            blobStorageService.uploadFileByMessage(file.toFile(), folder, msg)
                    );

                    switch (folder) {
                        case AppConstants.FOLDER_EMAIL -> emailFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, deliveryBlobUrl);
                        case AppConstants.FOLDER_MOBSTAT -> mobstatFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, deliveryBlobUrl);
                        case AppConstants.FOLDER_PRINT -> printFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, deliveryBlobUrl);
                    }

                    logger.info("[{}] ✅ Uploaded {} file for account {}: {}", msg.getBatchId(), folderToOutputMethod.get(folder), account, deliveryBlobUrl);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}", msg.getBatchId(), folderToOutputMethod.get(folder), account, e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed to scan folder {}: {}", msg.getBatchId(), folder, e.getMessage(), e);
        }
    }

    // -------- Build final summary list --------
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) continue;
        String account = customer.getAccountNumber();
        if (account == null || account.isBlank()) continue;

        Map<String, String> archiveMap = accountFileToArchiveUrl.getOrDefault(account, Collections.emptyMap());

        if (archiveMap.isEmpty()) {
            // No archive, add delivery-only entry
            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setPdfEmailFileUrl(emailFiles.getOrDefault(account, Collections.emptyMap()).values().stream().findFirst().orElse(null));
            entry.setPdfMobstatFileUrl(mobstatFiles.getOrDefault(account, Collections.emptyMap()).values().stream().findFirst().orElse(null));
            entry.setPrintFileUrl(printFiles.getOrDefault(account, Collections.emptyMap()).values().stream().findFirst().orElse(null));
            finalList.add(entry);
        } else {
            // One entry per archive file
            for (Map.Entry<String, String> archiveEntry : archiveMap.entrySet()) {
                String fileName = archiveEntry.getKey();
                String archiveUrl = archiveEntry.getValue();

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setArchiveBlobUrl(archiveUrl);

                // Match delivery files by same filename, if exist
                entry.setPdfEmailFileUrl(emailFiles.getOrDefault(account, Collections.emptyMap()).get(fileName));
                entry.setPdfMobstatFileUrl(mobstatFiles.getOrDefault(account, Collections.emptyMap()).get(fileName));
                entry.setPrintFileUrl(printFiles.getOrDefault(account, Collections.emptyMap()).get(fileName));

                finalList.add(entry);
            }
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    List<ProcessedFileEntry> allEntries = new ArrayList<>();

    for (SummaryProcessedFile file : processedFiles) {
        if (file == null) continue;

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());
        entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

        // Set individual statuses considering errors
        entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
        entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

        // Determine overall status
        boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
        boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
        boolean printSuccess = "SUCCESS".equals(entry.getPrintStatus());
        boolean archiveSuccess = "SUCCESS".equals(entry.getArchiveStatus());

        if ((emailSuccess && archiveSuccess) ||
            (mobstatSuccess && archiveSuccess && !emailSuccess && !printSuccess) ||
            (printSuccess && archiveSuccess && !emailSuccess && !mobstatSuccess)) {
            entry.setOverallStatus("SUCCESS");
        } else if (archiveSuccess) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }

        if (errors.size() > 0 && !"FAILED".equals(entry.getOverallStatus())) {
            entry.setOverallStatus("PARTIAL");
        }

        allEntries.add(entry);
    }

    return allEntries;
}
