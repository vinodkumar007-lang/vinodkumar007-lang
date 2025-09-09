private String findFileByAccount(Map<String, String> fileMap, String account) {
    if (account == null || fileMap == null) return null;

    return fileMap.entrySet().stream()
            .filter(e -> e.getKey().contains(account)) // match account
            .filter(e -> !e.getKey().endsWith(".tmp")) // skip temp files
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
}

private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) {
        logger.warn("[{}] ⚠️ jobDir, customerList, or msg is null", msg != null ? msg.getBatchId() : "N/A");
        return finalList;
    }

    List<String> allFolders = List.of(
            AppConstants.FOLDER_ARCHIVE,
            AppConstants.FOLDER_EMAIL,
            AppConstants.FOLDER_MOBSTAT,
            AppConstants.FOLDER_PRINT
    );

    Map<String, String> folderToOutputMethod = Map.of(
            AppConstants.FOLDER_EMAIL, AppConstants.OUTPUT_EMAIL,
            AppConstants.FOLDER_MOBSTAT, AppConstants.OUTPUT_MOBSTAT,
            AppConstants.FOLDER_PRINT, AppConstants.OUTPUT_PRINT
    );

    Map<String, Map<String, String>> folderFileMap = new HashMap<>();
    for (String folder : allFolders) {
        folderFileMap.put(folder, new HashMap<>());
        Path folderPath = jobDir.resolve(folder);

        logger.debug("[{}] Scanning folder: {} (exists={})", msg.getBatchId(), folderPath, Files.exists(folderPath));

        if (!Files.exists(folderPath)) {
            logger.warn("[{}] Folder not found: {}", msg.getBatchId(), folderPath);
            continue;
        }

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                String fileName = file.getFileName().toString();

                // Skip temporary files
                if (fileName.endsWith(".tmp")) {
                    logger.info("[{}] ⏩ Skipping temporary file: {} in folder {}", msg.getBatchId(), fileName, folder);
                    return;
                }

                try {
                    // Upload the file and map the URL
                    String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), folder, msg));
                    folderFileMap.get(folder).put(fileName, url);
                    logger.info("[{}] ✅ Uploaded {} file: {} | URL: {}", msg.getBatchId(), folder, fileName, url);

                } catch (NoSuchFileException nsfe) {
                    logger.warn("[{}] ⏩ File disappeared before upload, skipping: {} in folder {}", msg.getBatchId(), fileName, folder);
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(), folder, fileName, e.getMessage(), e);
                }
            });
        } catch (IOException e) {
            logger.error("[{}] ⚠️ Error walking folder {}: {}", msg.getBatchId(), folderPath, e.getMessage(), e);
        }
    }

    // Debug: print folderFileMap contents after scanning
    folderFileMap.forEach((folder, map) -> {
        logger.debug("[{}] Folder '{}' scanned files count={}", msg.getBatchId(), folder, map.size());
        map.forEach((fname, url) -> logger.debug("[{}]   {} -> {}", msg.getBatchId(), fname, url));
    });

    Set<String> uniqueKeys = new HashSet<>();
    boolean isMfc = "MFC".equalsIgnoreCase(msg.getSourceSystem());

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        Map<String, String> archivesForAccount = folderFileMap.getOrDefault(AppConstants.FOLDER_ARCHIVE, Collections.emptyMap());

        for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
            String archiveFileName = archiveEntry.getKey();
            String archiveUrl = archiveEntry.getValue();

            String key = customer.getCustomerId() + "|" + account + "|" + archiveFileName;
            if (uniqueKeys.contains(key)) continue;
            uniqueKeys.add(key);

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setArchiveBlobUrl(archiveUrl);

            // Mapping delivery files
            logger.debug("[{}] Mapping delivery files for customer {} | Account {}", msg.getBatchId(), customer.getCustomerId(), account);

            if (isMfc) {
                entry.setPdfEmailFileUrl(findFileByAccount(folderFileMap.get(AppConstants.FOLDER_EMAIL), account));
                entry.setPdfMobstatFileUrl(findFileByAccount(folderFileMap.get(AppConstants.FOLDER_MOBSTAT), account));
                entry.setPrintFileUrl(findFileByAccount(folderFileMap.get(AppConstants.FOLDER_PRINT), account));
            } else {
                entry.setPdfEmailFileUrl(folderFileMap.get(AppConstants.FOLDER_EMAIL).getOrDefault(
                        archiveFileName, findFileByAccount(folderFileMap.get(AppConstants.FOLDER_EMAIL), account)));
                entry.setPdfMobstatFileUrl(folderFileMap.get(AppConstants.FOLDER_MOBSTAT).getOrDefault(
                        archiveFileName, findFileByAccount(folderFileMap.get(AppConstants.FOLDER_MOBSTAT), account)));
                entry.setPrintFileUrl(folderFileMap.get(AppConstants.FOLDER_PRINT).getOrDefault(
                        archiveFileName, findFileByAccount(folderFileMap.get(AppConstants.FOLDER_PRINT), account)));
            }

            logger.debug("[{}] Customer {} | Account {} mapped files -> Email: {}, Mobstat: {}, Print: {}",
                    msg.getBatchId(), customer.getCustomerId(), account,
                    entry.getPdfEmailFileUrl(), entry.getPdfMobstatFileUrl(), entry.getPrintFileUrl());

            finalList.add(entry);
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}
