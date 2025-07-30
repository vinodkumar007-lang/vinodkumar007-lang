 private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        try {
            logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");
            logger.info("✅ Found XML file: {}", xmlFile);

            // ✅ Parse error report (already used in buildDetailedProcessedFiles)
            Map<String, Map<String, String>> errorMap = parseErrorReport(message);
            logger.info("🧾 Parsed error report with {} entries", errorMap.size());

            // ✅ Parse STDXML and extract basic customer summaries
            List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
            logger.info("📊 Total customerSummaries parsed: {}", customerSummaries.size());

            // ✅ Convert to basic SummaryProcessedFile list
            List<SummaryProcessedFile> customerList = customerSummaries.stream()
                    .map(cs -> {
                        SummaryProcessedFile spf = new SummaryProcessedFile();
                        spf.setAccountNumber(cs.getAccountNumber());
                        spf.setCustomerId(cs.getCisNumber());
                        return spf;
                    })
                    .collect(Collectors.toList());

            // ✅ Locate output job directory
            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

            // ✅ Build processedFiles with output-specific blob URLs and status (SUCCESS/ERROR)
            List<SummaryProcessedFile> processedFiles =
                    buildDetailedProcessedFiles(jobDir, customerList, errorMap, message);
            logger.info("📦 Processed {} customer records", processedFiles.size());

            // ✅ Upload print files and track their URLs
            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("🖨️ Uploaded {} print files", printFiles.size());

            // ✅ Upload MobStat trigger file if present
            String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
            logger.info("Found Mobstat {} url", mobstatTriggerUrl);
            // ✅ Extract counts from <outputList> inside STD XML
            Map<String, Integer> summaryCounts = extractSummaryCountsFromXml(xmlFile);
            // ✅ Create payload for summary.json
            String allFileNames = message.getBatchFiles().stream() .map(BatchFile::getFilename) .collect(Collectors.joining(", "));
            SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message,
                    processedFiles,      // ✅ Now includes blob URLs + status from buildDetailedProcessedFiles
                    allFileNames,
                    message.getBatchId(),
                    String.valueOf(message.getTimestamp()),
                    errorMap,
                    printFiles
            );
            if (payload.getHeader() != null) {
                payload.getHeader().setTimestamp(String.valueOf(message.getTimestamp()));
            }

            // ✅ Write and upload summary.json
            String fileName = "summary_" + message.getBatchId() + ".json";
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName);
            payload.setSummaryFileURL(decodeUrl(summaryUrl));
            logger.info("📁 Summary JSON uploaded to: {}", decodeUrl(summaryUrl));

            // ✅ Final beautified payload log
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
            response.setTimestamp(String.valueOf(message.getTimestamp()));

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                    new ApiResponse("Summary generated", "COMPLETED", response)));

            logger.info("✅ Kafka output sent for batch {} with response: {}", message.getBatchId(),
                    objectMapper.writeValueAsString(response));

        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
    }
