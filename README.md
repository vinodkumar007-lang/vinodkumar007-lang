private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
    try {
        logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
        File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
        if (xmlFile == null) throw new IllegalStateException("XML not found");
        logger.info("✅ Found XML file: {}", xmlFile);

        // ✅ Parse error report
        Map<String, Map<String, String>> errorMap = parseErrorReport(message);
        logger.info("🧾 Parsed error report with {} entries", errorMap.size());

        // ✅ Parse XML and get all customer summaries
        List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);

        // ✅ Build customerMap: accountNumber => SummaryProcessedFile
        Map<String, SummaryProcessedFile> customerMap = customerSummaries.stream()
                .collect(Collectors.toMap(
                        CustomerSummary::getAccountNumber,
                        cs -> {
                            SummaryProcessedFile spf = new SummaryProcessedFile();
                            spf.setAccountNumber(cs.getAccountNumber());
                            spf.setCisNumber(cs.getCisNumber());
                            return spf;
                        }
                ));

        // ✅ Set jobDir for output files
        Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

        // ✅ Build processed files with proper statuses and blob URLs
        List<SummaryProcessedFile> processedFiles =
                buildDetailedProcessedFiles(jobDir, customerMap, errorMap, message);
        logger.info("📦 Processed {} customer records", processedFiles.size());

        // ✅ Upload print files
        List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
        logger.info("🖨️ Uploaded {} print files", printFiles.size());

        // ✅ Upload mobstat trigger if present
        String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
        String currentTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());

        // ✅ Build final payload
        SummaryPayload payload = SummaryJsonWriter.buildPayload(
                message, processedFiles, printFiles, mobstatTriggerUrl, customerMap.size());

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
