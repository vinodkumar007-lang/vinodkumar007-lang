private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");

    Map<String, SummaryProcessedFile> outputMap = new HashMap<>();

    for (String folder : folders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> files = Files.list(folderPath)) {
            for (Path filePath : files.collect(Collectors.toList())) {
                String fileName = filePath.getFileName().toString();

                for (SummaryProcessedFile customer : customerList) {
                    String accountNumber = customer.getAccountNumber();
                    if (fileName.contains(accountNumber)) {
                        try {
                            SummaryProcessedFile entry = new SummaryProcessedFile();
                            BeanUtils.copyProperties(customer, entry);

                            String key = customer.getCustomerId() + "::" + accountNumber + "::" + customer.getOutputMethod();
                            String targetPath = String.format("out/%s/%s/%s", msg.getBatchId(), folder, fileName);
                            String blobUrl = blobStorageService.uploadFile(Files.readString(filePath), targetPath);

                            entry.setBlobURL(blobUrl);
                            entry.setStatus("SUCCESS");
                            outputMap.put(key, entry);
                            break; // stop once matched
                        } catch (Exception e) {
                            logger.error("❌ Error uploading file: {}", e.getMessage(), e);
                        }
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

    if (errorReportPath.isPresent()) {
        String content = Files.readString(errorReportPath.get());
        String errorReportBlobPath = String.format("out/%s/report/ErrorReport.txt", msg.getBatchId());
        String errorBlobUrl = blobStorageService.uploadFile(content, errorReportBlobPath);
        logger.info("📄 ErrorReport uploaded: {}", errorBlobUrl);

        Map<String, Map<String, String>> errorMap = parseErrorReport(content);

        errorMap.forEach((cust, errorInfo) -> {
            String account = errorInfo.get("account");
            String method = errorInfo.get("method");

            String key = cust + "::" + account + "::" + method;
            if (outputMap.containsKey(key)) {
                SummaryProcessedFile file = outputMap.get(key);
                file.setStatus("FAILED");
                file.setBlobURL(errorBlobUrl);
                logger.info("❗ ErrorReport matched - updated entry for {}", key);
            }
        });
    }

    return new ArrayList<>(outputMap.values());
}
