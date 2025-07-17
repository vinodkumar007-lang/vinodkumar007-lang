private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        Map<String, String> accountCustomerMap,
        KafkaMessage msg,
        ErrorReport errorReport
) throws IOException {
    Map<String, SummaryProcessedFile> processedMap = new HashMap<>();

    List<String> folders = List.of("archive", "email", "mobstat", "print");

    for (String folder : folders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> fileStream = Files.walk(folderPath)) {
            fileStream.filter(Files::isRegularFile).forEach(path -> {
                try {
                    String fileName = path.getFileName().toString();

                    // ✅ Skip mobstat_trigger.txt
                    if (fileName.equalsIgnoreCase("mobstat_trigger.txt")) return;

                    String accountNumber = extractAccountNumber(fileName);
                    String customerNumber = accountCustomerMap.getOrDefault(accountNumber, "");

                    if (accountNumber == null || customerNumber == null || accountNumber.isEmpty() || customerNumber.isEmpty()) {
                        logger.warn("❌ Could not determine account/customer for file: {}", fileName);
                        return;
                    }

                    String key = accountNumber + "_" + customerNumber;
                    SummaryProcessedFile spf = processedMap.getOrDefault(key, new SummaryProcessedFile());
                    spf.setAccountNumber(accountNumber);
                    spf.setCustomerNumber(customerNumber);

                    String blobUrl = blobStorageService.getBlobUrl(msg.getJobId(), folder + "/" + fileName);

                    switch (folder) {
                        case "archive" -> spf.setArchiveFileUrl(blobUrl);
                        case "email" -> spf.setEmailFileUrl(blobUrl);
                        case "mobstat" -> spf.setMobstatFileUrl(blobUrl);
                        case "print" -> spf.setPrintFileUrl(blobUrl);
                    }

                    // ✅ Status Logic:
                    // - If file is missing → check errorReport
                    // - If file in error report → Failed
                    // - If file not in error report → ""

                    File file = path.toFile();
                    if (!file.exists()) {
                        String method = extractMethod(fileName);
                        if (errorReport != null && errorReport.contains(fileName, method)) {
                            spf.setStatus("Failed");
                        } else {
                            spf.setStatus("");
                        }
                    } else {
                        spf.setStatus("Success");
                    }

                    processedMap.put(key, spf);

                } catch (Exception e) {
                    logger.error("❌ Error processing file in {}: {}", folder, e.getMessage(), e);
                }
            });
        }
    }

    return new ArrayList<>(processedMap.values());
}
