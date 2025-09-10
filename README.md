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
