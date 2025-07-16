private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
    try {
        logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
        File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
        if (xmlFile == null) throw new IllegalStateException("XML not found");
        logger.info("✅ Found XML file: {}", xmlFile);

        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
        doc.getDocumentElement().normalize();

        Map<String, String> accountCustomerMap = extractAccountCustomerMapFromDoc(doc);
        logger.info("\uD83D\uDCC4 Extracted {} customers from XML", accountCustomerMap.size());

        Map<String, SummaryProcessedFile> customerMap = new HashMap<>();
        accountCustomerMap.forEach((acc, cus) -> {
            SummaryProcessedFile spf = new SummaryProcessedFile();
            spf.setAccountNumber(acc);
            spf.setCustomerId(cus);
            customerMap.put(acc, spf);
        });

        Map<String, Map<String, String>> errorMap = parseErrorReport(message);
        logger.info("\uD83D\uDCC1 Parsed error report with {} entries", errorMap.size());

        Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());
        List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(jobDir, customerMap, errorMap, message);
        logger.info("📦 Processed {} customer records", processedFiles.size());

        List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
        logger.info("🖨️ Uploaded {} print files", printFiles.size());

        String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);

        // ✅ Set consistent timestamp
        String currentTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());

        // ✅ Build the payload
        SummaryPayload payload = SummaryJsonWriter.buildPayload(
                message, processedFiles, printFiles, mobstatTriggerUrl, accountCustomerMap.size());

        payload.setFileName(message.getBatchFiles().get(0).getFilename());
        payload.setTimestamp(currentTimestamp); // ✅ root-level timestamp

        if (payload.getHeader() != null) {
            payload.getHeader().setTimestamp(currentTimestamp); // ✅ header timestamp also set
        }

        // ✅ Upload to blob with clean path
        String fileName = "summary_" + message.getBatchId() + ".json";
        String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
        String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName); // returns clean URL

        payload.setSummaryFileURL(summaryUrl); // ✅ clean URL, not encoded

        logger.info("📁 Summary JSON uploaded to: {}", summaryUrl);
        logger.info("📄 Final Summary Payload:\n{}",
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

        SummaryResponse response = new SummaryResponse();
        response.setBatchID(message.getBatchId());
        response.setFileName(payload.getFileName());
        response.setHeader(payload.getHeader());
        response.setMetadata(payload.getMetadata());
        response.setPayload(payload.getPayload());
        response.setSummaryFileURL(summaryUrl);
        response.setTimestamp(currentTimestamp); // ✅ include in response if needed

        kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                new ApiResponse("Summary generated", "COMPLETED", response)));

        logger.info("✅ Kafka output sent for batch {} with response: {}", message.getBatchId(), objectMapper.writeValueAsString(response));

    } catch (Exception e) {
        logger.error("❌ Error post-OT summary generation", e);
    }
}
