private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
    try {
        logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
        File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
        if (xmlFile == null) throw new IllegalStateException("XML not found");

        logger.info("✅ Found XML file: {}", xmlFile.getAbsolutePath());
        Path jobDir = xmlFile.toPath().getParent().getParent().getParent(); // go 3 levels up
        logger.info("📁 JobDir resolved to: {}", jobDir);

        DataParser parser = new DataParser();
        SummaryPayload payload = parser.parse(xmlFile, message);

        // Read error report (filename may not end with .txt)
        File errorReport = Files.walk(jobDir)
                .filter(p -> p.getFileName().toString().startsWith("ErrorReport"))
                .map(Path::toFile)
                .findFirst()
                .orElse(null);

        Map<String, Map<String, String>> errorMap = new HashMap<>();
        if (errorReport != null && errorReport.exists()) {
            logger.info("📄 Reading ErrorReport: {}", errorReport.getAbsolutePath());
            try (BufferedReader reader = new BufferedReader(new FileReader(errorReport))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|");
                    if (parts.length >= 5) {
                        Map<String, String> info = new HashMap<>();
                        info.put("customer", parts[0]);
                        info.put("account", parts[1]);
                        info.put("error", parts[2]);
                        info.put("channel", parts[3]);
                        info.put("status", parts[4]);
                        errorMap.put(parts[1], info);
                    }
                }
            }
        } else {
            logger.warn("❌ ErrorReport file not found under {}", jobDir);
        }

        List<SummaryProcessedFile> enriched = buildDetailedProcessedFiles(
                jobDir, payload.getProcessedFiles(), errorMap, message);
        payload.setProcessedFiles(enriched);

        // Upload summary JSON and get Blob URL
        File summaryFile = SummaryJsonWriter.appendToSummaryJson(payload);
        String summaryBlobUrl = blobStorageService.uploadSummaryJson(summaryFile, message);
        payload.setSummaryFileURL(summaryBlobUrl);

        // ✅ Final Summary beautified print
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        logger.info("📄 Final Summary Payload:\n{}", mapper.writeValueAsString(payload));

        // 🔁 Send Kafka message
        sendToKafka(payload);

    } catch (Exception ex) {
        logger.error("🔥 Error processing after OT", ex);
        throw new RuntimeException("Error in post-processing", ex);
    }
}

private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "archive", "ARCHIVE",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    for (SummaryProcessedFile customer : customerList) {
        String accountId = customer.getAccountNumber();
        String outputMethod = customer.getOutputMethod();

        boolean fileFound = false;
        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            try (Stream<Path> stream = Files.walk(folderPath)) {
                Optional<Path> matchingFile = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(accountId))
                        .findFirst();

                if (matchingFile.isPresent()) {
                    Path file = matchingFile.get();
                    String blobUrl = blobStorageService.uploadFileAndReturnLocation(file, folder, msg);
                    SummaryProcessedFile entry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, entry);
                    entry.setBlobUrl(blobUrl);
                    entry.setStatus("OK");
                    finalList.add(entry);
                    fileFound = true;
                    break;
                }
            }
        }

        if (!fileFound) {
            boolean isFailed = false;
            boolean isSkipped = true;

            for (Map.Entry<String, Map<String, String>> entry : errorMap.entrySet()) {
                String errAcc = entry.getValue().getOrDefault("account", "");
                String errFolder = entry.getValue().getOrDefault("channel", "").toLowerCase();
                if (errAcc.contains(accountId)) {
                    String expectedFolder = outputMethod.toLowerCase();
                    if (errFolder.equals(expectedFolder)) {
                        isFailed = true;
                        isSkipped = false;
                        break;
                    }
                }
            }

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setBlobUrl(null);
            entry.setStatus(isFailed ? "FAILED" : "SKIPPED");
            finalList.add(entry);
        }
    }

    return finalList;
}
