 private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();

        if (!validateInputs(jobDir, customerList, msg)) {
            return finalList;
        }

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

        // Step 1: Upload all archive files
        Map<String, Map<String, String>> accountToArchiveMap = uploadArchiveFiles(jobDir, msg, errorMap);

        // Step 2: Upload delivery files
        Map<String, Map<String, String>> deliveryFileMaps = uploadDeliveryFiles(jobDir, deliveryFolders, folderToOutputMethod, msg, errorMap);

        // Step 3: Build final list
        finalList = buildFinalProcessedList(customerList, accountToArchiveMap, deliveryFileMaps, msg);

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }

    private boolean validateInputs(Path jobDir, List<SummaryProcessedFile> customerList, KafkaMessage msg) {
        if (jobDir == null) {
            logger.warn("[{}] ⚠️ jobDir is null. Skipping buildDetailedProcessedFiles.", msg != null ? msg.getBatchId() : "UNKNOWN");
            return false;
        }
        if (customerList == null || customerList.isEmpty()) {
            logger.warn("[{}] ⚠️ customerList is null/empty. Nothing to process.", msg != null ? msg.getBatchId() : "UNKNOWN");
            return false;
        }
        if (msg == null) {
            logger.warn("[UNKNOWN] ⚠️ KafkaMessage is null. Skipping buildDetailedProcessedFiles.");
            return false;
        }
        return true;
    }

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

    private Map<String, Map<String, String>> uploadDeliveryFiles(
            Path jobDir,
            List<String> deliveryFolders,
            Map<String, String> folderToOutputMethod,
            KafkaMessage msg,
            Map<String, Map<String, String>> errorMap) throws IOException {

        Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();
        for (String folder : deliveryFolders) {
            deliveryFileMaps.put(folder.toLowerCase(), new HashMap<>());
        }

        logger.info("[{}] 🚀 Starting delivery file upload. jobDir: {}, folders: {}",
                msg.getBatchId(), jobDir, deliveryFolders);

        // Walk entire jobDir recursively to find all directories
        List<Path> allDirs;
        try (Stream<Path> stream = Files.walk(jobDir)) {
            allDirs = stream.filter(Files::isDirectory).toList();
        }

        // Log all discovered directories for diagnostics
        logger.info("[{}] 📂 All discovered directories under jobDir:", msg.getBatchId());
        allDirs.forEach(d -> logger.info("   - {}", d));

        // Filter directories that match delivery folders (case-insensitive, anywhere in path)
        List<Path> folderPaths = allDirs.stream()
                .filter(p -> deliveryFolders.stream()
                        .anyMatch(f -> p.toString().toLowerCase().contains(f.toLowerCase())))
                .toList();

        // Log warning for any missing delivery folder
        for (String folder : deliveryFolders) {
            boolean found = folderPaths.stream()
                    .anyMatch(p -> p.toString().toLowerCase().contains(folder.toLowerCase()));
            if (!found) {
                logger.warn("[{}] ⚠️ Delivery folder '{}' not found under jobDir: {}",
                        msg.getBatchId(), folder, jobDir);
            }
        }

        // Process files in each detected folder
        for (Path folderPath : folderPaths) {
            String folderName = folderPath.getFileName().toString().trim();
            String folderKey = deliveryFolders.stream()
                    .filter(f -> folderPath.toString().toLowerCase().contains(f.toLowerCase()))
                    .findFirst()
                    .orElse(folderName.toLowerCase());

            logger.info("[{}] 🔎 Processing delivery folder: {} (key={})",
                    msg.getBatchId(), folderPath, folderKey);

            try (Stream<Path> files = Files.walk(folderPath)) {
                List<Path> fileList = files
                        .filter(Files::isRegularFile)
                        .filter(f -> !f.getFileName().toString().endsWith(".tmp"))
                        .toList();

                if (fileList.isEmpty()) {
                    logger.warn("[{}] ⚠️ No files found in folder: {}", msg.getBatchId(), folderPath);
                    continue;
                }

                logger.info("[{}] Found {} file(s) in folder {}", msg.getBatchId(), fileList.size(), folderName);

                for (Path file : fileList) {
                    logger.info("[{}] 📂 Found file to upload: {}", msg.getBatchId(), file.getFileName());
                    processDeliveryFile(file, folderKey, folderToOutputMethod, msg, deliveryFileMaps, errorMap);
                }
            }
        }

        logger.info("[{}] ✅ Finished delivery file upload. Result: {}", msg.getBatchId(), deliveryFileMaps);
        return deliveryFileMaps;
    }

    private void processDeliveryFile(Path file, String folderKey,
                                     Map<String, String> folderToOutputMethod,
                                     KafkaMessage msg,
                                     Map<String, Map<String, String>> deliveryFileMaps,
                                     Map<String, Map<String, String>> errorMap) {

        if (!Files.exists(file)) {
            logger.info("[{}] ⏩ Skipping missing file: {} in folder {}", msg.getBatchId(), file, folderKey);
            return;
        }

        String fileName = file.getFileName().toString();
        String normalizedKey = folderKey.toLowerCase(); // normalize once

        deliveryFileMaps.computeIfAbsent(normalizedKey, k -> new HashMap<>());

        try {
            // Upload file
            String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), normalizedKey, msg));
            deliveryFileMaps.get(normalizedKey).put(fileName, url);

            logger.info("[{}] ✅ Uploaded {} file: {}",
                    msg.getBatchId(),
                    folderToOutputMethod.getOrDefault(normalizedKey, normalizedKey),
                    url);

        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed to upload file {} in folder {}: {}",
                    msg.getBatchId(), fileName, normalizedKey, e.getMessage(), e);

            errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                    .put(fileName, folderToOutputMethod.getOrDefault(normalizedKey, normalizedKey)
                            + " upload failed: " + e.getMessage());
        }
    }
