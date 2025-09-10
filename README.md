private Map<String, Map<String, String>> uploadDeliveryFiles(
        Path jobDir,
        List<String> deliveryFolders,
        Map<String, String> folderToOutputMethod,
        KafkaMessage msg,
        Map<String, Map<String, String>> errorMap) throws IOException {

    // Normalize deliveryFolders to lowercase for case-insensitive matching
    List<String> normalizedFolders = deliveryFolders.stream()
            .map(String::toLowerCase)
            .toList();

    Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();
    for (String folder : deliveryFolders) {
        deliveryFileMaps.put(folder.toLowerCase(), new HashMap<>());
    }

    // Walk entire jobDir recursively to find all matching delivery folders
    try (Stream<Path> allDirs = Files.walk(jobDir)) {
        List<Path> folderPaths = allDirs
                .filter(Files::isDirectory)
                .filter(p -> normalizedFolders.contains(p.getFileName().toString().toLowerCase()))
                .toList();

        if (folderPaths.isEmpty()) {
            logger.warn("[{}] ⚠️ No delivery folders found under jobDir: {}", msg.getBatchId(), jobDir);
            return deliveryFileMaps;
        }

        for (Path folderPath : folderPaths) {
            String folderName = folderPath.getFileName().toString();
            String folderKey = folderName.toLowerCase(); // use lowercase as key
            logger.info("[{}] 🔎 Processing delivery folder: {}", msg.getBatchId(), folderPath);

            try (Stream<Path> files = Files.walk(folderPath)) {
                files.filter(Files::isRegularFile)
                        .filter(f -> !f.getFileName().toString().endsWith(".tmp"))
                        .forEach(file -> {
                            logger.debug("[{}] 📂 Found {} file: {}", msg.getBatchId(), folderName, file.getFileName());
                            processDeliveryFile(file, folderKey, folderToOutputMethod, msg, deliveryFileMaps, errorMap);
                        });
            }
        }
    }

    return deliveryFileMaps;
}

private void processDeliveryFile(Path file, String folderKey,
                                 Map<String, String> folderToOutputMethod,
                                 KafkaMessage msg,
                                 Map<String, Map<String, String>> deliveryFileMaps,
                                 Map<String, Map<String, String>> errorMap) {
    if (!Files.exists(file)) {
        logger.warn("[{}] ⏩ Skipping missing {} file: {}", msg.getBatchId(), folderKey, file);
        return;
    }

    String fileName = file.getFileName().toString();
    try {
        String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), folderKey, msg));
        deliveryFileMaps.get(folderKey).put(fileName, url);

        logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(),
                folderToOutputMethod.getOrDefault(folderKey, folderKey), url);
    } catch (Exception e) {
        logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(),
                folderToOutputMethod.getOrDefault(folderKey, folderKey), fileName, e.getMessage(), e);
        errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                .put(fileName, folderToOutputMethod.getOrDefault(folderKey, folderKey) + " upload failed: " + e.getMessage());
    }
}
