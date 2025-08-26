// ===============================
// 1️⃣ Build detailed processed files per customer (all archive files included)
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

    // -------- Upload all archive files and map by account -> List of URLs --------
    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    Map<String, List<String>> accountToArchiveUrls = new HashMap<>();
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
                    accountToArchiveUrls.computeIfAbsent(account, k -> new ArrayList<>())
                                        .add(archiveBlobUrl);

                    logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveBlobUrl);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed scanning ARCHIVE folder: {}", msg.getBatchId(), e.getMessage(), e);
        }
    }

    // -------- Upload delivery files per account (email/mobstat/print) --------
    Map<String, String> emailUrls = new HashMap<>();
    Map<String, String> mobstatUrls = new HashMap<>();
    Map<String, String> printUrls = new HashMap<>();

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
                        case AppConstants.FOLDER_EMAIL -> emailUrls.put(account, deliveryBlobUrl);
                        case AppConstants.FOLDER_MOBSTAT -> mobstatUrls.put(account, deliveryBlobUrl);
                        case AppConstants.FOLDER_PRINT -> printUrls.put(account, deliveryBlobUrl);
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

    // -------- Build final list per archive file --------
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) continue;
        String account = customer.getAccountNumber();
        if (account == null || account.isBlank()) continue;

        List<String> archives = accountToArchiveUrls.getOrDefault(account, Collections.emptyList());
        if (archives.isEmpty()) {
            // If no archive, still create one entry
            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setPdfEmailFileUrl(emailUrls.get(account));
            entry.setPdfMobstatFileUrl(mobstatUrls.get(account));
            entry.setPrintFileUrl(printUrls.get(account));
            finalList.add(entry);
        } else {
            // Create one entry per archive file
            for (String archiveUrl : archives) {
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setArchiveBlobUrl(archiveUrl);
                entry.setPdfEmailFileUrl(emailUrls.get(account));
                entry.setPdfMobstatFileUrl(mobstatUrls.get(account));
                entry.setPrintFileUrl(printUrls.get(account));
                finalList.add(entry);
            }
        }
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

    List<ProcessedFileEntry> allEntries = new ArrayList<>();

    for (SummaryProcessedFile file : processedFiles) {
        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());
        entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

        entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
        entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

        // Overall status
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

        // Mark PARTIAL if errors exist
        if (errorMap.containsKey(file.getAccountNumber()) && !"FAILED".equals(entry.getOverallStatus())) {
            entry.setOverallStatus("PARTIAL");
        }

        allEntries.add(entry);
    }

    return allEntries;
}
