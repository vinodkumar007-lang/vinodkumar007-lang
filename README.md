private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles) {

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

        // Check for errors for this account
        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

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

        if (archiveSuccess && (emailSuccess || mobstatSuccess || printSuccess)) {
            entry.setOverallStatus("SUCCESS");
        } else if (archiveSuccess) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }

        if (errorMap.containsKey(file.getAccountNumber()) && !"FAILED".equals(entry.getOverallStatus())) {
            entry.setOverallStatus("PARTIAL");
        }

        allEntries.add(entry);
    }
==========

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

    // --- Map archive files by filename ---
    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    Map<String, String> filenameToArchiveUrl = new HashMap<>();
    if (Files.exists(archivePath)) {
        try (Stream<Path> stream = Files.walk(archivePath)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                String filename = file.getFileName().toString();
                try {
                    String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg));
                    filenameToArchiveUrl.put(filename, url);
                    logger.info("[{}] 📦 Uploaded archive file {} => {}", msg.getBatchId(), filename, url);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), filename, e.getMessage(), e);
                }
            });
        }
    }

    // --- Map delivery files by filename ---
    Map<String, String> filenameToEmailUrl = new HashMap<>();
    Map<String, String> filenameToMobstatUrl = new HashMap<>();
    Map<String, String> filenameToPrintUrl = new HashMap<>();

    for (String folder : deliveryFolders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                String filename = file.getFileName().toString();
                try {
                    String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), folder, msg));
                    switch (folder) {
                        case AppConstants.FOLDER_EMAIL -> filenameToEmailUrl.put(filename, url);
                        case AppConstants.FOLDER_MOBSTAT -> filenameToMobstatUrl.put(filename, url);
                        case AppConstants.FOLDER_PRINT -> filenameToPrintUrl.put(filename, url);
                    }
                    logger.info("[{}] ✅ Uploaded {} file {} => {}", msg.getBatchId(), folder, filename, url);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(), folder, filename, e.getMessage(), e);
                }
            });
        }
    }

    // --- Build final entries: one per archive filename ---
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) continue;

        for (Map.Entry<String, String> archiveEntry : filenameToArchiveUrl.entrySet()) {
            String archiveFilename = archiveEntry.getKey();
            String archiveUrl = archiveEntry.getValue();

            // Only include files that match customer's account prefix
            if (!archiveFilename.startsWith(customer.getAccountNumber())) continue;

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setArchiveBlobUrl(archiveUrl);
            entry.setPdfEmailFileUrl(filenameToEmailUrl.getOrDefault(archiveFilename, null));
            entry.setPdfMobstatFileUrl(filenameToMobstatUrl.getOrDefault(archiveFilename, null));
            entry.setPrintFileUrl(filenameToPrintUrl.getOrDefault(archiveFilename, null));
            finalList.add(entry);
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

    return allEntries;
}
