private Map<String, Map<String, String>> uploadDeliveryFiles(
        Path jobDir,
        List<String> deliveryFolders,
        Map<String, String> folderToOutputMethod,
        KafkaMessage msg,
        Map<String, Map<String, String>> errorMap) throws IOException {

    Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();
    for (String folder : deliveryFolders) {
        deliveryFileMaps.put(folder, new HashMap<>());
    }

    // Walk entire jobDir recursively to find all matching delivery folders
    try (Stream<Path> allDirs = Files.walk(jobDir)) {
        List<Path> folderPaths = allDirs
                .filter(Files::isDirectory)
                .filter(p -> deliveryFolders.contains(p.getFileName().toString()))
                .toList();

        if (folderPaths.isEmpty()) {
            logger.warn("[{}] ⚠️ No delivery folders found under jobDir: {}", msg.getBatchId(), jobDir);
            return deliveryFileMaps;
        }

        for (Path folderPath : folderPaths) {
            String folderName = folderPath.getFileName().toString();
            logger.info("[{}] 🔎 Processing delivery folder: {}", msg.getBatchId(), folderPath);

            try (Stream<Path> files = Files.walk(folderPath)) {
                files.filter(Files::isRegularFile)
                     .filter(f -> !f.getFileName().toString().endsWith(".tmp"))
                     .forEach(file -> {
                         logger.debug("[{}] 📂 Found {} file: {}", 
                                 msg.getBatchId(), folderName, file.getFileName());
                         processDeliveryFile(file, folderName, folderToOutputMethod, msg, deliveryFileMaps, errorMap);
                     });
            }
        }
    }

    return deliveryFileMaps;
}
