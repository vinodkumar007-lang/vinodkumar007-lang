has message.getSourceSystem() been sanitised?

private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        String batchId = message.getBatchId(); // golden thread
        try {
            logger.info("[{}] ⏳ Waiting for XML for jobId={}, id={}", batchId, otResponse.getJobId(), otResponse.getId());
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");

            logger.info("[{}] ✅ Found XML file: {}", batchId, xmlFile);

            Map<String, Map<String, String>> errorMap = parseErrorReport(message);
            logger.info("[{}] 🧾 Parsed error report with {} entries", batchId, errorMap.size());

            List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
            logger.info("[{}] 📊 Total customerSummaries parsed: {}", batchId, customerSummaries.size());

            List<SummaryProcessedFile> customerList = customerSummaries.stream()
                    .map(cs -> {
                        SummaryProcessedFile spf = new SummaryProcessedFile();
                        spf.setAccountNumber(cs.getAccountNumber());
                        spf.setCustomerId(cs.getCisNumber());
                        return spf;
                    })
                    .collect(Collectors.toList());

            Path jobDir = Paths.get(mountPath, AppConstants.OUTPUT_FOLDER, message.getSourceSystem(), otResponse.getJobId());
            logger.info("[{}] 📂 Resolved jobDir path = {}", batchId, jobDir.toAbsolutePath());
            logger.info("[{}] 🔄 Invoking buildDetailedProcessedFiles...", batchId);
            List<SummaryProcessedFile> processedFiles =
                    buildDetailedProcessedFiles(jobDir, customerList, errorMap, message);
            logger.info("[{}] 📦 Processed {} customer records", batchId, processedFiles.size());

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("[{}] 🖨️ Uploaded {} print files", batchId, printFiles.size());

            String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
            logger.info("[{}] 📱 Found Mobstat URL: {}", batchId, mobstatTriggerUrl);

            Map<String, Integer> summaryCounts = extractSummaryCountsFromXml(xmlFile);

            String allFileNames = message.getBatchFiles().stream()
                    .map(BatchFile::getFilename)
                    .collect(Collectors.joining(", "));

            SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message, processedFiles, allFileNames, batchId,
                    String.valueOf(message.getTimestamp()), errorMap, printFiles
            );

            if (payload.getHeader() != null) {
                payload.getHeader().setTimestamp(String.valueOf(message.getTimestamp()));
            }

            String fileName = AppConstants.SUMMARY_FILENAME_PREFIX + batchId + AppConstants.JSON_EXTENSION;
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName);
            payload.setSummaryFileURL(decodeUrl(summaryUrl));
            logger.info("[{}] 📁 Summary JSON uploaded to: {}", batchId, decodeUrl(summaryUrl));

            logger.info("[{}] 📄 Final Summary Payload:\n{}", batchId,
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(batchId);
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setPayload(payload.getPayload());
            response.setSummaryFileURL(decodeUrl(summaryUrl));
            response.setTimestamp(String.valueOf(message.getTimestamp()));

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                    new ApiResponse("Summary generated", "COMPLETED", response)));

            logger.info("[{}] ✅ Kafka output sent with response: {}", batchId,
                    objectMapper.writeValueAsString(response));

        } catch (Exception e) {
            logger.error("[{}] ❌ Error post-OT summary generation: {}", batchId, e.getMessage(), e);
        }
    }
