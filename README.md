String blobUrl = "";
String deliveryStatus = "SUCCESS"; // Default

if (Files.exists(methodPath)) {
    Optional<Path> match = Files.list(methodPath)
            .filter(Files::isRegularFile)
            .filter(p -> p.getFileName().toString().contains(account))
            .findFirst();

    if (match.isPresent()) {
        blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
    } else {
        // File not found — check errorMap
        Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());
        String errorStatus = customerErrors.getOrDefault(outputMethod, null);
        if ("FAILED".equalsIgnoreCase(errorStatus)) {
            deliveryStatus = "FAILED";
        }
    }
} else {
    // Folder not found — still check errorMap
    Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());
    String errorStatus = customerErrors.getOrDefault(outputMethod, null);
    if ("FAILED".equalsIgnoreCase(errorStatus)) {
        deliveryStatus = "FAILED";
    }
}
