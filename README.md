@KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}", containerFactory = "kafkaListenerContainerFactory")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        logger.info("\uD83D\uDCE5 Received Kafka message");
        try {
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            logger.info("\uD83D\uDD0E Parsed Kafka message with batchId: {}", message.getBatchId());

            List<BatchFile> dataFiles = message.getBatchFiles().stream()
                    .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                    .toList();
            message.setBatchFiles(dataFiles);
            logger.info("\uD83D\uDCC4 Filtered DATA files: {}", dataFiles.size());

            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);
            logger.info("\uD83D\uDCC1 Created input directory: {}", batchDir);

            for (BatchFile file : dataFiles) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(message.getJobName() + ".csv");
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
                logger.info("⬇️ Downloaded blob file to: {}", localPath);
            }

            writeAndUploadMetadataJson(message, batchDir);

            logger.info("\uD83D\uDCE4 Calling OT orchestration API");
            OTResponse otResponse = callOrchestrationBatchApi("TOKEN", message);
            if (otResponse == null) {
                logger.error("❌ OT orchestration API failed");
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"OT call failed\"}");
                ack.acknowledge();
                return;
            }

            Map<String, Object> pendingMsg = Map.of(
                    "batchID", batchId,
                    "status", "PENDING",
                    "message", "OT Request Sent"
            );
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(pendingMsg));
            logger.info("\uD83D\uDFE1 OT request sent and acknowledged with PENDING status");

            ack.acknowledge();

            executor.submit(() -> processAfterOT(message, otResponse));

        } catch (Exception ex) {
            logger.error("❌ Kafka processing failed", ex);
        }
    }

    private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        logger.info("⏳ Starting post-OT processing for jobId={}, batchId={}...", otResponse.getJobId(), message.getBatchId());
        try {
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");

            logger.info("\uD83D\uDCC1 Parsing XML file: {}", xmlFile.getAbsolutePath());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromDoc(doc);
            logger.info("\uD83D\uDD0D Extracted {} customer entries from XML", accountCustomerMap.size());

            int customersProcessed = 0;
            NodeList outputListNodes = doc.getElementsByTagName("outputList");
            if (outputListNodes.getLength() > 0) {
                Element outputList = (Element) outputListNodes.item(0);
                String val = outputList.getAttribute("customersProcessed");
                if (val != null) {
                    try {
                        customersProcessed = Integer.parseInt(val);
                    } catch (NumberFormatException ignored) {}
                }
            }

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

            List<SummaryProcessedFile> processedFiles = buildAndUploadProcessedFiles(jobDir, accountCustomerMap, message);
            logger.info("\uD83D\uDCE6 Uploaded {} processed files", processedFiles.size());

            String errorReportPath = Paths.get(jobDir.toString(), "ErrorReport.csv").toString();
            List<SummaryProcessedFile> failures = appendFailureEntries(errorReportPath, processedFiles);
            processedFiles = mergeStatuses(processedFiles, failures);
            logger.info("⚠️ Merged {} failure entries into processed list", failures.size());

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("\uD83D\uDDB8️ Uploaded {} print files", printFiles.size());

            String triggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();
            if (Files.exists(Paths.get(triggerPath))) {
                blobStorageService.uploadFile(new File(triggerPath), message.getSourceSystem() + "/mobstat_trigger/DropData.trigger");
                logger.info("\uD83D\uDE80 Trigger file uploaded to blob: {}", triggerPath);
            }

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, triggerPath, customersProcessed);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + message.getBatchId() + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            logger.info("\uD83D\uDCC4 Summary JSON built and uploaded: {}", payload.getSummaryFileURL());
            logger.info("\uD83D\uDCC4 Summary JSON content: \n{}", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(message.getBatchId());
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setPayload(payload.getPayload());
            response.setSummaryFileURL(payload.getSummaryFileURL());

            ApiResponse finalResponse = new ApiResponse("Summary generated", "COMPLETED", response);
            logger.info("\uD83D\uDCE4 Final response sent to Kafka:");
            logger.info(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalResponse));

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(finalResponse));
            logger.info("✅ Summary successfully published to output topic");

        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
    }

    private List<SummaryProcessedFile> mergeStatuses(List<SummaryProcessedFile> successes, List<SummaryProcessedFile> failures) {
        Map<String, SummaryProcessedFile> map = new HashMap<>();
        for (SummaryProcessedFile file : successes) {
            map.put(file.getAccountNumber(), file);
        }
        for (SummaryProcessedFile fail : failures) {
            map.merge(fail.getAccountNumber(), fail, (existing, newFail) -> {
                if (newFail.getPdfEmailFileUrl() != null) existing.setPdfEmailFileUrl("FAILED");
                if (newFail.getHtmlEmailFileUrl() != null) existing.setHtmlEmailFileUrl("FAILED");
                if (newFail.getPdfMobstatFileUrl() != null) existing.setPdfMobstatFileUrl("FAILED");
                if (newFail.getTxtEmailFileUrl() != null) existing.setTxtEmailFileUrl("FAILED");
                if (newFail.getPdfArchiveFileUrl() != null) existing.setPdfArchiveFileUrl("FAILED");
                existing.setStatusCode("PARTIAL");
                existing.setStatusDescription("Partially processed");
                return existing;
            });
        }
        return new ArrayList<>(map.values());
    }
