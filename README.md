private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");
    Map<String, SummaryProcessedFile> outputMap = new HashMap<>();

    Set<String> validFolders = new HashSet<>();
    AtomicReference<String> triggerBlobUrl = new AtomicReference<>();

    // Upload .trigger file if found
    try (Stream<Path> allFiles = Files.walk(jobDir)) {
        allFiles.filter(Files::isRegularFile).forEach(path -> {
            String fileName = path.getFileName().toString();
            if (fileName.endsWith(".trigger")) {
                try {
                    String targetPath = String.format(
                            "%s/%s/%s/%s",                     // sourceSystem/batchId/guid/filename
                            msg.getSourceSystem(),
                            msg.getBatchId(),
                            msg.getUniqueConsumerRef(),
                            fileName
                    );
                    byte[] content = Files.readAllBytes(path);
                    triggerBlobUrl.set(blobStorageService.uploadFile(content, targetPath));
                    logger.info("📎 Trigger file uploaded: {}", triggerBlobUrl);
                } catch (Exception e) {
                    logger.error("❌ Failed to upload trigger file: {}", e.getMessage());
                }
            }
        });
    }

    // Upload actual folder files first
    for (String folder : folders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        validFolders.add(folder);
        try (Stream<Path> files = Files.list(folderPath)) {
            for (Path filePath : files.collect(Collectors.toList())) {
                String fileName = filePath.getFileName().toString();
                if (fileName.endsWith(".trigger")) continue; // skip

                for (SummaryProcessedFile customer : customerList) {
                    String accountNumber = customer.getAccountNumber();
                    String customerId = customer.getCustomerId();
                    String outputMethod = customer.getOutputMethod();
                    String key = customerId + "::" + accountNumber + "::" + outputMethod;

                    if (outputMap.containsKey(key)) continue;

                    if (fileName.contains(accountNumber)) {
                        try {
                            SummaryProcessedFile entry = buildCopy(customer);
                            String blobPath = String.format(
                                    "%s/%s/%s/%s/%s",
                                    msg.getSourceSystem(),
                                    msg.getBatchId(),
                                    msg.getUniqueConsumerRef(),
                                    folder,
                                    fileName
                            );
                            byte[] content = Files.readAllBytes(filePath);
                            String blobUrl = blobStorageService.uploadFile(content, blobPath);
                            entry.setBlobURL(decodeUrl(blobUrl));
                            entry.setStatus("SUCCESS");
                            outputMap.put(key, entry);
                        } catch (Exception e) {
                            logger.error("❌ Error uploading file: {}", e.getMessage());
                        }
                        break;
                    }
                }
            }
        }
    }

    // Process ErrorReport
    Path reportDir = jobDir.resolve("report");
    Optional<Path> errorReportPath = Files.exists(reportDir)
            ? Files.list(reportDir).filter(p -> p.getFileName().toString().contains("ErrorReport")).findFirst()
            : Optional.empty();

    Map<String, Map<String, String>> errorMap = new HashMap<>();
    String errorBlobUrl = null;

    if (errorReportPath.isPresent()) {
        String content = Files.readString(errorReportPath.get());
        String errorReportBlobPath = String.format(
                "%s/%s/%s/report/ErrorReport.txt",
                msg.getSourceSystem(),
                msg.getBatchId(),
                msg.getUniqueConsumerRef()
        );
        errorBlobUrl = blobStorageService.uploadFile(content, errorReportBlobPath);
        logger.info("📄 ErrorReport uploaded: {}", errorBlobUrl);
        errorMap = parseErrorReport(content);
    }

    // Final pass to ensure all customers handled
    for (SummaryProcessedFile customer : customerList) {
        String customerId = customer.getCustomerId();
        String accountNumber = customer.getAccountNumber();
        String outputMethod = customer.getOutputMethod();
        String key = customerId + "::" + accountNumber + "::" + outputMethod;

        if (outputMap.containsKey(key)) continue;

        SummaryProcessedFile entry = buildCopy(customer);

        boolean matchedError = errorMap.containsKey(customerId)
                && errorMap.get(customerId).get("account").equals(accountNumber)
                && errorMap.get(customerId).get("method").equalsIgnoreCase(outputMethod);

        if (matchedError) {
            entry.setStatus("FAILED");
            entry.setStatusDescription("Marked as failed from ErrorReport");
            entry.setBlobURL(errorBlobUrl);
        } else if (validFolders.contains(outputMethod.toLowerCase())) {
            entry.setStatus("NOT_FOUND");
            entry.setStatusDescription("File not found in expected folder");
        } else {
            entry.setStatus("NOT_FOUND");
            entry.setStatusDescription("Output method folder not generated");
        }

        outputMap.put(key, entry);
    }

    // Set overallStatus
    outputMap.values().forEach(entry -> {
        String status = entry.getStatus();
        switch (status) {
            case "SUCCESS":
                entry.setOverallStatus("SUCCESS");
                break;
            case "FAILED":
                entry.setOverallStatus("FAILURE");
                break;
            default:
                entry.setOverallStatus("NOT_FOUND");
        }
    });

    return new ArrayList<>(outputMap.values());
}
