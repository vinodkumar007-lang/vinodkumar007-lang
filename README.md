private Map<String, Map<String, String>> uploadDeliveryFiles(
        Path jobDir,
        List<String> deliveryFolders,
        Map<String, String> folderToOutputMethod,
        KafkaMessage msg,
        Map<String, Map<String, String>> errorMap) throws IOException {

    // Dynamically initialize maps for each delivery folder
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

I’ve removed the dedicated getOtdsToken() and now rely on the existing getSecret() method to fetch the OTDS token the same way as other Key Vault secrets. This keeps everything consistent and avoids duplicating logic.

Agreed – I’ve updated the code so deliveryFileMaps is initialized dynamically based on the deliveryFolders parameter instead of hardcoding folder names. This makes it easy to add new delivery types with a single config change, no code changes required.

