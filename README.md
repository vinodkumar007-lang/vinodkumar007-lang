Why not copy the folder names from the input deliveryFolders parameter? That way it is a single change to add a new folder


private Map<String, Map<String, String>> uploadDeliveryFiles(
            Path jobDir,
            List<String> deliveryFolders,
            Map<String, String> folderToOutputMethod,
            KafkaMessage msg,
            Map<String, Map<String, String>> errorMap) throws IOException {

        Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();
        deliveryFileMaps.put(AppConstants.FOLDER_EMAIL, new HashMap<>());
        deliveryFileMaps.put(AppConstants.FOLDER_MOBSTAT, new HashMap<>());
        deliveryFileMaps.put(AppConstants.FOLDER_PRINT, new HashMap<>());

        for (String folder : deliveryFolders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) {
                logger.debug("[{}] ℹ️ Delivery folder not found: {}", msg.getBatchId(), folder);
                continue;
            }

            try (Stream<Path> stream = Files.walk(folderPath)) {
                stream.filter(Files::isRegularFile)
                        .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                        .forEach(file -> processDeliveryFile(file, folder, folderToOutputMethod, msg, deliveryFileMaps, errorMap));
            }
        }

        return deliveryFileMaps;
    }
