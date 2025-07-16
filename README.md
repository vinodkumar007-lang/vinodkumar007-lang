1001179722|191749661002|CASA|error|Failed
5898460774023071|600006709118|CreditCard|error|Failed

private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            Map<String, SummaryProcessedFile> customerMap,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> result = new ArrayList<>();
        List<String> folders = List.of("email", "archive", "mobstat", "print");
        Map<String, String> folderToOutputMethod = Map.of(
                "email", "EMAIL",
                "archive", "ARCHIVE",
                "mobstat", "MOBSTAT",
                "print", "PRINT"
        );

        for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
            String account = entry.getKey();
            SummaryProcessedFile spf = entry.getValue();
            Map<String, Boolean> methodFound = new HashMap<>();

            for (String folder : folders) {
                Path folderPath = jobDir.resolve(folder);
                Optional<Path> fileOpt = Files.exists(folderPath)
                        ? Files.list(folderPath)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst()
                        : Optional.empty();

                String outputMethod = folderToOutputMethod.get(folder);
                Map<String, String> errorEntry = errorMap.getOrDefault(account, Collections.emptyMap());
                String failureStatus = errorEntry.getOrDefault(outputMethod, "");

                boolean fileFound = fileOpt.isPresent();
                methodFound.put(outputMethod, fileFound);

                if (fileFound) {
                    Path file = fileOpt.get();
                    String blobUrl = blobStorageService.uploadFile(
                            file.toFile(),
                            msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName()
                    );
                    String decoded = decodeUrl(blobUrl);

                    switch (folder) {
                        case "email" -> {
                            spf.setPdfEmailFileUrl(decoded);
                            spf.setPdfEmailStatus("OK");
                        }
                        case "archive" -> {
                            spf.setPdfArchiveFileUrl(decoded);
                            spf.setPdfArchiveStatus("OK");
                        }
                        case "mobstat" -> {
                            spf.setPdfMobstatFileUrl(decoded);
                            spf.setPdfMobstatStatus("OK");
                        }
                        case "print" -> spf.setPrintFileUrl(decoded);
                    }
                } else {
                    // file not found, check error report
                    boolean isExplicitlyFailed = "Failed".equalsIgnoreCase(failureStatus);
                    if (isExplicitlyFailed) {
                        switch (folder) {
                            case "email" -> spf.setPdfEmailStatus("Failed");
                            case "archive" -> spf.setPdfArchiveStatus("Failed");
                            case "mobstat" -> spf.setPdfMobstatStatus("Failed");
                        }
                    } else {
                        // if not in error report, leave status as empty
                        switch (folder) {
                            case "email" -> spf.setPdfEmailStatus("");
                            case "archive" -> spf.setPdfArchiveStatus("");
                            case "mobstat" -> spf.setPdfMobstatStatus("");
                        }
                    }
                }
            }

            // Final status decision logic
            List<String> statuses = Arrays.asList(
                    spf.getPdfEmailStatus(),
                    spf.getPdfArchiveStatus(),
                    spf.getPdfMobstatStatus()
            );

            long failedCount = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
            long knownCount = statuses.stream().filter(s -> s != null && !s.isBlank()).count();

            if (failedCount == knownCount && knownCount > 0) {
                spf.setStatusCode("FAILED");
                spf.setStatusDescription("All methods failed");
            } else if (failedCount > 0) {
                spf.setStatusCode("PARTIAL");
                spf.setStatusDescription("Some methods failed");
            } else {
                spf.setStatusCode("SUCCESS");
                spf.setStatusDescription("Success");
            }

            result.add(spf);
        }

        return result;
    }

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
                                spf.setCustomerId(cs.getCisNumber());
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
