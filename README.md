private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "archive", "ARCHIVE",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    Map<String, SummaryProcessedFile> outputMap = new HashMap<>();

    // 🟢 Step 1: Try to find file for each customer
    for (SummaryProcessedFile customer : customerList) {
        String key = customer.getCustomerId() + "::" + customer.getAccountNumber() + "::" + customer.getOutputMethod();
        boolean fileFound = false;

        for (String folder : folders) {
            if (!folderToOutputMethod.get(folder).equalsIgnoreCase(customer.getOutputMethod())) continue;

            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            try (Stream<Path> files = Files.list(folderPath)) {
                Optional<Path> matchFile = files
                        .filter(p -> {
                            String fileName = p.getFileName().toString();
                            return fileName.contains(customer.getCustomerId()) &&
                                   fileName.contains(customer.getAccountNumber());
                        })
                        .findFirst();

                if (matchFile.isPresent()) {
                    Path filePath = matchFile.get();
                    String targetPath = String.format("out/%s/%s/%s", msg.getBatchId(), folder, filePath.getFileName());
                    String blobUrl = blobStorageService.uploadFile(Files.readString(filePath), targetPath);

                    SummaryProcessedFile entry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, entry);
                    entry.setBlobURL(blobUrl);
                    entry.setStatus("SUCCESS");

                    outputMap.put(key, entry);
                    fileFound = true;
                    break;
                }
            }
        }

        // 🔴 Step 2: If not found, put a temporary FAILED entry (updated later via ErrorReport)
        if (!fileFound) {
            SummaryProcessedFile failedEntry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, failedEntry);
            failedEntry.setStatus("FAILED"); // Will add blobURL in next step
            outputMap.put(key, failedEntry);
        }
    }

    // 📄 Step 3: Handle ErrorReport
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

        for (Map.Entry<String, Map<String, String>> errorEntry : errorMap.entrySet()) {
            String cust = errorEntry.getKey();
            String account = errorEntry.getValue().get("account");
            String method = errorEntry.getValue().get("method");

            String key = cust + "::" + account + "::" + method;
            if (outputMap.containsKey(key)) {
                SummaryProcessedFile entry = outputMap.get(key);
                if ("FAILED".equalsIgnoreCase(entry.getStatus())) {
                    entry.setBlobURL(errorBlobUrl); // Set error blob
                }
            }
        }
    }

    return new ArrayList<>(outputMap.values());
}
