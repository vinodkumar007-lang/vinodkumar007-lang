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

private void processDeliveryFile(Path file, String folderName,
                                     Map<String, String> folderToOutputMethod,
                                     KafkaMessage msg,
                                     Map<String, Map<String, String>> deliveryFileMaps,
                                     Map<String, Map<String, String>> errorMap) {

        if (!Files.exists(file)) {
            logger.info("[{}] ⏩ Skipping missing file: {} in folder {}", msg.getBatchId(), file, folderName);
            return;
        }

        String fileName = file.getFileName().toString();
        String folderKey = folderName.toLowerCase(); // key in map

        deliveryFileMaps.computeIfAbsent(folderKey, k -> new HashMap<>());

        try {
            // Upload file
            String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), folderName, msg));
            deliveryFileMaps.get(folderKey).put(fileName, url);

            logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(),
                    folderToOutputMethod.getOrDefault(folderName, folderName), url);
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed to upload file {} in folder {}: {}", msg.getBatchId(), fileName, folderName, e.getMessage(), e);
            errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                    .put(fileName, folderToOutputMethod.getOrDefault(folderName, folderName) + " upload failed: " + e.getMessage());
        }
    }
