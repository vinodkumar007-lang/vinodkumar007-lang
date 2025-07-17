private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage message
) {
    Map<String, SummaryProcessedFile> customerMap = new LinkedHashMap<>();
    List<String> folders = List.of("archive", "email", "html", "mobstat", "txt");

    for (String folder : folders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> files = Files.walk(folderPath)) {
            files.filter(Files::isRegularFile).forEach(path -> {
                try {
                    String fileName = path.getFileName().toString();
                    if (!fileName.contains("_")) return;

                    // Extract customer and accountNumber
                    String[] parts = fileName.split("_");
                    if (parts.length < 2) return;
                    String customer = parts[0];
                    String accountNumber = parts[1].split("\\.")[0]; // remove extension

                    String key = customer + "-" + accountNumber;

                    SummaryProcessedFile entry = customerMap.getOrDefault(key, new SummaryProcessedFile());
                    entry.setCustomer(customer);
                    entry.setAccountNumber(accountNumber);

                    // Extract error details if present
                    Map<String, String> errDetails = errorMap.getOrDefault(customer + accountNumber, new HashMap<>());
                    entry.setStatus(errDetails.getOrDefault("status", "SUCCESS"));
                    entry.setErrorCode(errDetails.get("errorCode"));
                    entry.setErrorDescription(errDetails.get("errorDescription"));

                    // Assign URL to correct delivery type
                    String blobUrl = blobStorageService.uploadProcessedFile(path.toString(), message);
                    switch (folder.toLowerCase()) {
                        case "archive" -> entry.setArchiveURL(blobUrl);
                        case "email" -> entry.setEmailURL(blobUrl);
                        case "html" -> entry.setHtmlURL(blobUrl);
                        case "mobstat" -> entry.setMobstatURL(blobUrl);
                        case "txt" -> entry.setTxtURL(blobUrl);
                    }

                    customerMap.put(key, entry);
                } catch (Exception e) {
                    logger.error("⚠️ Failed to process file in {}: {}", folder, path, e);
                }
            });
        } catch (IOException e) {
            logger.error("⚠️ Failed to walk folder: {}", folderPath, e);
        }
    }

    return new ArrayList<>(customerMap.values());
}
