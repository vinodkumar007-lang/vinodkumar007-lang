 private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        try {
            logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");
            logger.info("✅ Found XML file: {}", xmlFile);

            // ✅ Parse error report
            Map<String, Map<String, String>> errorMap = parseErrorReport(message);
            logger.info("🧾 Parsed error report with {} entries", errorMap.size());

            List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
            logger.info("\uD83D\uDCCA Total customerSummaries parsed: {}", customerSummaries.size());

            List<SummaryProcessedFile> customerList = customerSummaries.stream()
                    .map(cs -> {
                        SummaryProcessedFile spf = new SummaryProcessedFile();
                        spf.setAccountNumber(cs.getAccountNumber());
                        spf.setCustomerId(cs.getCisNumber());
                        return spf;
                    })
                    .collect(Collectors.toList());

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

            List<SummaryProcessedFile> processedFiles =
                    buildDetailedProcessedFiles(jobDir, customerList, errorMap, message);
            logger.info("\uD83D\uDCE6 Processed {} customer records", processedFiles.size());
            // ✅ Upload print files
            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("🖨️ Uploaded {} print files", printFiles.size());

            // ✅ Upload mobstat trigger if present
            String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
            String currentTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
// ⬇️ Extract customersProcessed and pagesProcessed from XML <outputList>
            Map<String, Integer> summaryCounts = extractSummaryCountsFromXml(xmlFile);
            int customersProcessed = summaryCounts.getOrDefault("customersProcessed", processedFiles.size());
            int pagesProcessed = summaryCounts.getOrDefault("pagesProcessed", 0);
            // ✅ Build final payload
            SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message,
                    processedFiles,
                    pagesProcessed,
                    printFiles,
                    mobstatTriggerUrl,
                    customersProcessed // ✅ now using extracted value
            );
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

        Map<String, SummaryProcessedFile> outputMap = new LinkedHashMap<>();

        for (SummaryProcessedFile spf : customerList) {
            String account = spf.getAccountNumber();
            String customer = spf.getCustomerId();
            if (account == null || account.isBlank()) continue;

            String key = customer + "::" + account;
            SummaryProcessedFile entry = outputMap.getOrDefault(key, new SummaryProcessedFile());
            BeanUtils.copyProperties(spf, entry);

            for (String folder : folders) {
                String outputMethod = folderToOutputMethod.get(folder);

                Path folderPath = jobDir.resolve(folder);
                Path matchedFile = null;
                if (Files.exists(folderPath)) {
                    try (Stream<Path> files = Files.list(folderPath)) {
                        matchedFile = files
                                .filter(p -> p.getFileName().toString().contains(account))
                                .filter(p -> !(folder.equals("mobstat") && p.getFileName().toString().toLowerCase().contains("trigger")))
                                .findFirst()
                                .orElse(null);
                    } catch (IOException e) {
                        logger.warn("⚠️ Could not scan folder '{}': {}", folder, e.getMessage());
                    }
                }

                String failureStatus = errorMap.getOrDefault(account, Collections.emptyMap()).getOrDefault(outputMethod, "");

                if (matchedFile != null) {
                    String blobUrl = blobStorageService.uploadFile(
                            matchedFile.toFile(),
                            msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + matchedFile.getFileName()
                    );
                    String decoded = decodeUrl(blobUrl);

                    switch (folder) {
                        case "email" -> {
                            entry.setPdfEmailFileUrl(decoded);
                            entry.setPdfEmailStatus("OK");
                        }
                        case "archive" -> {
                            entry.setPdfArchiveFileUrl(decoded);
                            entry.setPdfArchiveStatus("OK");
                        }
                        case "mobstat" -> {
                            entry.setPdfMobstatFileUrl(decoded);
                            entry.setPdfMobstatStatus("OK");
                        }
                        case "print" -> {
                            entry.setPrintFileUrl(decoded);
                            entry.setPrintStatus("OK");
                        }
                    }
                } else {
                    boolean isExplicitFail = "Failed".equalsIgnoreCase(failureStatus);
                    switch (folder) {
                        case "email" -> entry.setPdfEmailStatus(isExplicitFail ? "Failed" : "Skipped");
                        case "archive" -> entry.setPdfArchiveStatus(isExplicitFail ? "Failed" : "Skipped");
                        case "mobstat" -> entry.setPdfMobstatStatus(isExplicitFail ? "Failed" : "Skipped");
                        case "print" -> entry.setPrintStatus(isExplicitFail ? "Failed" : "Skipped");
                    }
                }
            }

            // ✅ Determine overall status
            List<String> statuses = Arrays.asList(
                    entry.getPdfEmailStatus(),
                    entry.getPdfArchiveStatus(),
                    entry.getPdfMobstatStatus(),
                    entry.getPrintStatus()
            );

            long failed = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
            long skipped = statuses.stream().filter("Skipped"::equalsIgnoreCase).count();
            long success = statuses.stream().filter("OK"::equalsIgnoreCase).count();

            if (success > 0 && (failed > 0 || skipped > 0)) {
                entry.setStatusCode("PARTIAL");
                entry.setStatusDescription("Some outputs failed or skipped");
            } else if (failed > 0 && success == 0) {
                entry.setStatusCode("FAILED");
                entry.setStatusDescription("All outputs failed");
            } else if (success == 0 && skipped > 0) {
                entry.setStatusCode("SKIPPED");
                entry.setStatusDescription("No files found");
            } else {
                entry.setStatusCode("SUCCESS");
                entry.setStatusDescription("All outputs successful");
            }

            outputMap.put(key, entry);
        }

        // ✅ Include additional failures from errorMap not in customerList
        for (String account : errorMap.keySet()) {
            for (String outputMethod : errorMap.get(account).keySet()) {
                boolean exists = outputMap.values().stream()
                        .anyMatch(e -> account.equals(e.getAccountNumber()));
                if (!exists) {
                    SummaryProcessedFile err = new SummaryProcessedFile();
                    err.setAccountNumber(account);
                    err.setCustomerId("UNKNOWN");

                    switch (outputMethod) {
                        case "EMAIL" -> err.setPdfEmailStatus("Failed");
                        case "ARCHIVE" -> err.setPdfArchiveStatus("Failed");
                        case "MOBSTAT" -> err.setPdfMobstatStatus("Failed");
                        case "PRINT" -> err.setPrintStatus("Failed");
                    }

                    err.setStatusCode("FAILED");
                    err.setStatusDescription("Failed due to error report");
                    outputMap.put("error::" + account, err);
                }
            }
        }

        return new ArrayList<>(outputMap.values());
    }
    
