private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
    try {
        logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
        File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
        if (xmlFile == null) throw new IllegalStateException("XML not found");
        logger.info("✅ Found XML file: {}", xmlFile);

        // ✅ Parse error report
        Map<String, Map<String, String>> errorMap = parseErrorReport(message);
        logger.info("🧾 Parsed error report with {} entries", errorMap.size());

        // ✅ Parse STD XML to get initial customer list
        List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
        logger.info("📊 Total customerSummaries parsed: {}", customerSummaries.size());

        // ✅ Prepare unique customer+account list to track
        Map<String, SummaryProcessedFile> customerMap = new HashMap<>();
        for (CustomerSummary cs : customerSummaries) {
            String key = cs.getCisNumber() + "_" + cs.getAccountNumber();
            SummaryProcessedFile spf = new SummaryProcessedFile();
            spf.setCustomerId(cs.getCisNumber());
            spf.setAccountNumber(cs.getAccountNumber());
            customerMap.put(key, spf);
        }

        Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

        // ✅ Build processedFiles with merged folder URLs (archive/email/mobstat)
        List<SummaryProcessedFile> processedFiles =
                buildDetailedProcessedFiles(jobDir, new ArrayList<>(customerMap.values()), errorMap, message);

        logger.info("📦 Processed {} merged customer records", processedFiles.size());

        // ✅ Upload print files
        List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
        logger.info("🖨️ Uploaded {} print files", printFiles.size());

        // ✅ Upload mobstat trigger if present
        String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
        if (mobstatTriggerUrl != null) {
            logger.info("📤 Uploaded mobstat trigger file: {}", mobstatTriggerUrl);
        }

        String currentTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());

        // ✅ Build final summary payload (exclude mobstat trigger in processed count)
        int actualCustomerCount = processedFiles.size();
        SummaryPayload payload = SummaryJsonWriter.buildPayload(
                message, processedFiles, printFiles, mobstatTriggerUrl, actualCustomerCount);

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

        // ✅ Send final response to Kafka
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
