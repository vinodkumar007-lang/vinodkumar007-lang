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

        // ✅ Extract counts from <outputList> inside STD XML
        Map<String, Integer> summaryCounts = extractSummaryCountsFromXml(xmlFile);
        int customersProcessed = summaryCounts.getOrDefault("customersProcessed", processedFiles.size());
        int pagesProcessed = summaryCounts.getOrDefault("pagesProcessed", 0);

        // ✅ Create payload for summary.json
        SummaryPayload payload = SummaryJsonWriter.buildPayload(
                message,
                processedFiles,      // ✅ Now includes blob URLs + status from buildDetailedProcessedFiles
                pagesProcessed,
                printFiles,
                mobstatTriggerUrl,
                customersProcessed
        );

        String currentTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        payload.setFileName(message.getBatchFiles().get(0).getFilename());
        payload.setTimestamp(currentTimestamp);
        if (payload.getHeader() != null) {
            payload.getHeader().setTimestamp(currentTimestamp);
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

    for (String folder : folders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> files = Files.list(folderPath)) {
            for (Path filePath : files.collect(Collectors.toList())) {
                String fileName = filePath.getFileName().toString();
                Optional<SummaryProcessedFile> match = customerList.stream()
                        .filter(c -> fileName.contains(c.getCustomer()) &&
                                     fileName.contains(c.getAccount()) &&
                                     folderToOutputMethod.get(folder).equalsIgnoreCase(c.getOutputMethod()))
                        .findFirst();

                match.ifPresent(customer -> {
                    try {
                        SummaryProcessedFile entry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, entry);

                        String key = customer.getCustomer() + "::" + customer.getAccount() + "::" + customer.getOutputMethod();
                        String targetPath = String.format("out/%s/%s/%s", msg.getBatchID(), folder, fileName);
                        String blobUrl = uploadFile(Files.readString(filePath), targetPath);

                        entry.setBlobUrl(blobUrl);
                        entry.setStatus("SUCCESS");
                        outputMap.put(key, entry);
                    } catch (Exception e) {
                        logger.error("❌ Error uploading file: {}", e.getMessage(), e);
                    }
                });
            }
        }
    }

    // 🔍 Process ErrorReport file and update failed statuses
    Path reportDir = jobDir.resolve("report");
    Optional<Path> errorReportPath = Files.exists(reportDir)
            ? Files.list(reportDir).filter(p -> p.getFileName().toString().contains("ErrorReport")).findFirst()
            : Optional.empty();

    if (errorReportPath.isPresent()) {
        String content = Files.readString(errorReportPath.get());
        String errorReportBlobPath = String.format("out/%s/report/ErrorReport.txt", msg.getBatchID());
        String errorBlobUrl = uploadFile(content, errorReportBlobPath);
        logger.info("📄 ErrorReport uploaded: {}", errorBlobUrl);

        Map<String, Map<String, String>> errorMap = parseErrorReport(content);

        // Update error statuses in outputMap
        errorMap.forEach((cust, errorInfo) -> {
            String account = errorInfo.get("account");
            String method = errorInfo.get("method");

            String key = cust + "::" + account + "::" + method;
            if (outputMap.containsKey(key)) {
                SummaryProcessedFile file = outputMap.get(key);
                file.setStatus("FAILED");
                file.setBlobUrl(errorBlobUrl); // ✅ Pointing to error blob
                logger.info("❗ ErrorReport matched - updated entry for {}", key);
            }
        });
    }

    return new ArrayList<>(outputMap.values());
}

private Map<String, Map<String, String>> parseErrorReport(String content) {
    Map<String, Map<String, String>> errorMap = new HashMap<>();

    for (String line : content.split("\\r?\\n")) {
        String[] parts = line.split("\\|");
        if (parts.length >= 5) {
            String customer = parts[0].trim();
            String account = parts[1].trim();
            String method = parts[3].trim().toUpperCase(); // EMAIL, PRINT, etc.

            Map<String, String> info = new HashMap<>();
            info.put("account", account);
            info.put("method", method);

            errorMap.put(customer, info);
        }
    }
    return errorMap;
}

