private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
    try {
        logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
        File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
        if (xmlFile == null) throw new IllegalStateException("XML not found");
        logger.info("✅ Found XML file: {}", xmlFile);

        // ✅ Parse error report
        Map<String, Map<String, String>> errorMap = parseErrorReport(message);
        logger.info("🧾 Parsed error report with {} entries", errorMap.size());

        // ✅ Build customer-account map
        Map<String, Map<String, String>> accountCustomerMap = buildCustomerAccountMap(message);
        logger.info("👥 Customer-Account map built with {} entries", accountCustomerMap.size());

        // ✅ Parse STD XML for customer summaries
        List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
        logger.info("📊 Total customerSummaries parsed: {}", customerSummaries.size());

        // ✅ Build jobDir path
        Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

        // ✅ Build processed files
        List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(jobDir, errorMap, accountCustomerMap, message);
        logger.info("📦 Processed {} customer records", processedFiles.size());

        // ✅ Upload print files
        List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
        logger.info("🖨️ Uploaded {} print files", printFiles.size());

        // ✅ Upload mobstat trigger if present
        String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
        String currentTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());

        // ✅ Build final payload
        SummaryPayload payload = SummaryJsonWriter.buildPayload(
            message, processedFiles, printFiles, mobstatTriggerUrl, processedFiles.size());

        payload.setFileName(message.getBatchFiles().get(0).getFilename());
        payload.setTimestamp(currentTimestamp);
        if (payload.getHeader() != null) {
            payload.getHeader().setTimestamp(currentTimestamp);
        }

        // ✅ Upload summary.json
        String fileName = "summary_" + message.getBatchId() + ".json";
        String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
        String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName);
        payload.setSummaryFileURL(decodeUrl(summaryUrl));

        logger.info("📁 Summary JSON uploaded to: {}", decodeUrl(summaryUrl));
        logger.info("📄 Final Summary Payload:\n{}",
            objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

        // ✅ Send response to Kafka
        SummaryResponse response = new SummaryResponse();
        response.setBatchID(message.getBatchId());
        response.setFileName(payload.getFileName());
        response.setHeader(payload.getHeader());
        response.setMetadata(payload.getMetadata());
        response.setPayload(payload.getPayload());
        response.setSummaryFileURL(decodeUrl(summaryUrl));
        response.setTimestamp(currentTimestamp);

        kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
            new ApiResponse("Summary generated", "COMPLETED", response)));

        logger.info("✅ Kafka output sent for batch {} with response: {}", message.getBatchId(),
            objectMapper.writeValueAsString(response));

    } catch (Exception e) {
        logger.error("❌ Error post-OT summary generation", e);
    }
}

private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        Map<String, Map<String, String>> errorMap,
        Map<String, Map<String, String>> accountCustomerMap,
        KafkaMessage message
) {
    List<SummaryProcessedFile> summaryFiles = new ArrayList<>();
    List<String> folders = List.of("archive", "email", "mobstat", "print");

    for (String folder : folders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> files = Files.list(folderPath)) {
            files.filter(Files::isRegularFile).forEach(path -> {
                String filename = path.getFileName().toString();
                String key = filename.replaceAll("\\..*", ""); // Remove extension

                Map<String, String> customerMeta = accountCustomerMap.getOrDefault(key, Map.of());
                Map<String, String> errors = errorMap.getOrDefault(key, Map.of());

                SummaryProcessedFile file = new SummaryProcessedFile();
                file.setBlobURL(path.toString());
                file.setCustomerId(customerMeta.getOrDefault("customerId", ""));
                file.setAccountNumber(customerMeta.getOrDefault("accountNumber", ""));
                file.setChannel(folder.toUpperCase());
                file.setErrorCode(errors.getOrDefault("code", null));
                file.setErrorDescription(errors.getOrDefault("description", null));

                summaryFiles.add(file);
            });
        } catch (IOException e) {
            logger.error("❌ Error reading folder: {}", folderPath, e);
        }
    }
    return summaryFiles;
}

private Map<String, Map<String, String>> buildCustomerAccountMap(KafkaMessage message) {
    Map<String, Map<String, String>> map = new HashMap<>();
    for (CustomerData data : message.getCustomerData()) {
        String key = data.getFileName().replaceAll("\\..*", ""); // key like 12345
        Map<String, String> meta = new HashMap<>();
        meta.put("customerId", data.getCustomerId());
        meta.put("accountNumber", data.getAccountNumber());
        map.put(key, meta);
    }
    return map;
}
