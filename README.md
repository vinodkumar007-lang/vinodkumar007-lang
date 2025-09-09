/**
 * Returns all delivery file URLs matching the account.
 */
private List<String> findFilesByAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null || fileMap.isEmpty() || account == null) {
        return Collections.emptyList();
    }

    String normalizedAccount = account.trim().toLowerCase();

    return fileMap.entrySet().stream()
            .filter(e -> e.getKey() != null && e.getKey().toLowerCase().contains(normalizedAccount))
            .map(Map.Entry::getValue)
            .toList();
}

/**
 * Uploads all files from the specified delivery folders and returns a map of folder -> (filename -> file URL).
 */
private Map<String, Map<String, String>> uploadDeliveryFiles(
        Path jobDir,
        List<String> deliveryFolders,
        Map<String, String> folderToOutputMethod,
        KafkaMessage msg,
        Map<String, Map<String, String>> errorMap
) throws IOException {

    Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();

    for (String folder : deliveryFolders) {
        deliveryFileMaps.put(folder, new HashMap<>());
        Path folderPath = jobDir.resolve(folder);

        if (!Files.exists(folderPath)) {
            continue;
        }

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                  .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                  .forEach(file -> {
                      String fileName = file.getFileName().toString(); // only filename
                      try {
                          // Upload file and get the full URL
                          String uploadedUrl = blobStorageService.uploadFileByMessage(file.toFile(), folder, msg);
                          String decodedUrl = decodeUrl(uploadedUrl);

                          // Store in map: key = filename, value = URL
                          deliveryFileMaps.get(folder).put(fileName, decodedUrl);

                          logger.info("[{}] ✅ Uploaded {} file: {}", 
                                  msg.getBatchId(),
                                  folderToOutputMethod.getOrDefault(folder, folder), 
                                  decodedUrl);

                      } catch (Exception e) {
                          logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", 
                                  msg.getBatchId(),
                                  folderToOutputMethod.getOrDefault(folder, folder), 
                                  fileName, 
                                  e.getMessage(), e);

                          errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                                  .put(fileName, folderToOutputMethod.getOrDefault(folder, folder) + 
                                          " upload failed: " + e.getMessage());
                      }
                  });
        }
    }

    return deliveryFileMaps;
}
