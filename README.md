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

        for (String folder : deliveryFolders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) {
                logger.debug("[{}] ℹ️ Delivery folder not found: {}", msg.getBatchId(), folder);
                continue;
            }

            try (Stream<Path> stream = Files.walk(folderPath)) {
                stream.filter(Files::isRegularFile)
                        .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                        .forEach(file -> processDeliveryFile(
                                file, folder, folderToOutputMethod, msg, deliveryFileMaps, errorMap));
            }
        }

        return deliveryFileMaps;
    }

    private void processDeliveryFile(Path file, String folder,
                                     Map<String, String> folderToOutputMethod,
                                     KafkaMessage msg,
                                     Map<String, Map<String, String>> deliveryFileMaps,
                                     Map<String, Map<String, String>> errorMap) {
        if (!Files.exists(file)) {
            logger.warn("[{}] ⏩ Skipping missing {} file: {}", msg.getBatchId(), folder, file);
            return;
        }

        String fileName = file.getFileName().toString();
        try {
            String url = decodeUrl(
                    blobStorageService.uploadFileByMessage(file.toFile(), folder, msg)
            );
            deliveryFileMaps.get(folder).put(fileName, url);

            logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(),
                    folderToOutputMethod.get(folder), url);
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(),
                    folderToOutputMethod.getOrDefault(folder, folder), fileName, e.getMessage(), e);
            errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                    .put(fileName, folderToOutputMethod.getOrDefault(folder, folder) + " upload failed: " + e.getMessage());
        }
    }
