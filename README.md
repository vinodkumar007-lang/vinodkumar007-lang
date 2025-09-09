private Map<String, Map<String, String>> uploadArchiveFiles(Path jobDir, KafkaMessage msg, Map<String, Map<String, String>> errorMap) throws IOException {
        Map<String, Map<String, String>> accountToArchiveMap = new HashMap<>();
        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);

        if (!Files.exists(archivePath)) {
            logger.warn("[{}] ⚠️ Archive folder does not exist: {}", msg.getBatchId(), archivePath);
            return accountToArchiveMap;
        }

        try (Stream<Path> stream = Files.walk(archivePath)) {
            stream.filter(Files::isRegularFile)
                    .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                    .forEach(file -> processArchiveFile(file, msg, accountToArchiveMap, errorMap));
        }

        return accountToArchiveMap;
    }

    private void processArchiveFile(Path file, KafkaMessage msg,
                                    Map<String, Map<String, String>> accountToArchiveMap,
                                    Map<String, Map<String, String>> errorMap) {
        if (!Files.exists(file)) {
            logger.warn("[{}] ⏩ Skipping missing archive file: {}", msg.getBatchId(), file);
            return;
        }

        String fileName = file.getFileName().toString();
        String account = extractAccountFromFileName(fileName);
        if (account == null) {
            logger.debug("[{}] ⚠️ Skipping archive file without account mapping: {}", msg.getBatchId(), fileName);
            return;
        }

        try {
            String archiveUrl = decodeUrl(
                    blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg)
            );
            accountToArchiveMap
                    .computeIfAbsent(account, k -> new HashMap<>())
                    .put(fileName, archiveUrl);

            logger.info("[{}] 📦 Uploaded archive file for account [{}]: {}", msg.getBatchId(), account, archiveUrl);
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
            errorMap.computeIfAbsent(account, k -> new HashMap<>())
                    .put(fileName, "Archive upload failed: " + e.getMessage());
        }
    }
