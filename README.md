/**
 * Returns all delivery file URLs matching the account.
 * Added debug logs for troubleshooting.
 */
private List<String> findFilesByAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null || fileMap.isEmpty() || account == null) {
        logger.warn("findFilesByAccount called with empty map or null account");
        return Collections.emptyList();
    }

    String normalizedAccount = account.trim().toLowerCase();

    // Log all available keys for debugging
    logger.debug("Available files in map: {}", fileMap.keySet());
    logger.debug("Searching for account: {}", normalizedAccount);

    List<String> matchedFiles = fileMap.entrySet().stream()
            .filter(e -> e.getKey() != null && e.getKey().toLowerCase().contains(normalizedAccount))
            .peek(e -> logger.debug("Matched file: {} -> {}", e.getKey(), e.getValue()))
            .map(Map.Entry::getValue)
            .toList();

    if (matchedFiles.isEmpty()) {
        logger.warn("No files matched for account: {}", account);
    } else {
        logger.info("Matched {} files for account: {}", matchedFiles.size(), account);
    }

    return matchedFiles;
}

/**
 * Uploads all files from the specified delivery folders and returns a map of folder -> (filename -> file URL).
 * Includes detailed debug logs to track every file.
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

        logger.info("[{}] Scanning folder: {}", msg.getBatchId(), folderPath.toAbsolutePath());

        if (!Files.exists(folderPath)) {
            logger.warn("[{}] Folder does not exist: {}", msg.getBatchId(), folder);
            continue;
        }

        try (Stream<Path> stream = Files.walk(folderPath)) {
            stream.filter(Files::isRegularFile)
                  .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                  .forEach(file -> {
                      String fileName = file.getFileName().toString(); // only filename
                      logger.debug("[{}] Found file: {}", msg.getBatchId(), fileName);

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
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Error scanning folder {}: {}", msg.getBatchId(), folder, e.getMessage(), e);
        }

        logger.info("[{}] Total files uploaded in folder {}: {}", msg.getBatchId(), folder, deliveryFileMaps.get(folder).size());
    }

    return deliveryFileMaps;
}
