private ApiResponse processSingleMessage(KafkaMessage message) throws UnsupportedEncodingException {
    if (message == null) {
        return new ApiResponse("Empty message", "error",
                new SummaryPayloadResponse("Empty message", "error", new SummaryResponse()).getSummaryResponse());
    }

    Header header = new Header();
    header.setTenantCode(message.getTenantCode());
    header.setChannelID(message.getChannelID());
    header.setAudienceID(message.getAudienceID());
    header.setTimestamp(instantToIsoString(message.getTimestamp()));
    header.setSourceSystem(message.getSourceSystem());
    header.setProduct(message.getProduct());
    header.setJobName(message.getJobName());

    Payload payload = new Payload();
    payload.setUniqueConsumerRef(message.getUniqueConsumerRef());
    payload.setRunPriority(message.getRunPriority());
    payload.setEventType(message.getEventType());

    List<SummaryProcessedFile> processedFiles = new ArrayList<>();
    List<PrintFile> printFiles = new ArrayList<>();
    Metadata metadata = new Metadata();
    String summaryFileUrl = "";
    int fileCount = 0;

    String fileName = null;
    if (message.getBatchFiles() != null && !message.getBatchFiles().isEmpty()) {
        String firstBlobUrl = message.getBatchFiles().get(0).getBlobUrl();
        String blobPath = extractBlobPath(firstBlobUrl);
        fileName = extractFileName(blobPath);
    }
    if (fileName == null || fileName.isEmpty()) {
        fileName = message.getBatchId() + "_summary.json";
    }

    for (BatchFile file : message.getBatchFiles()) {
        try {
            // ✅ Copy the blob to Trigger folder
            String originalBlobUrl = file.getBlobUrl();
            String originalFileName = extractFileName(extractBlobPath(originalBlobUrl));
            String triggerBlobPath = String.format("%s/Trigger/%s", message.getSourceSystem(), originalFileName);
            String copiedTriggerUrl = blobStorageService.copyFileFromUrlToBlob(originalBlobUrl, triggerBlobPath);

            // ✅ Read content from original blob file
            String inputFileContent = blobStorageService.downloadFileContent(originalFileName);
            List<CustomerData> customers = DataParser.extractCustomerData(inputFileContent);
            if (customers.isEmpty()) continue;

            for (CustomerData customer : customers) {
                File pdfFile = FileGenerator.generatePdf(customer);
                File htmlFile = FileGenerator.generateHtml(customer);
                File txtFile = FileGenerator.generateTxt(customer);
                File mobstatFile = FileGenerator.generateMobstat(customer);

                String pdfArchiveUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "archive",
                                customer.getAccountNumber(), pdfFile.getName())).split("\\?")[0];

                String pdfEmailUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "email",
                                customer.getAccountNumber(), pdfFile.getName())).split("\\?")[0];

                String htmlEmailUrl = blobStorageService.uploadFile(htmlFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "html",
                                customer.getAccountNumber(), htmlFile.getName())).split("\\?")[0];

                String txtEmailUrl = blobStorageService.uploadFile(txtFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "txt",
                                customer.getAccountNumber(), txtFile.getName())).split("\\?")[0];

                String mobstatUrl = blobStorageService.uploadFile(mobstatFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "mobstat",
                                customer.getAccountNumber(), mobstatFile.getName())).split("\\?")[0];

                SummaryProcessedFile processedFile = new SummaryProcessedFile();
                processedFile.setCustomerId(customer.getCustomerId());
                processedFile.setAccountNumber(customer.getAccountNumber());
                processedFile.setPdfArchiveFileUrl(URLDecoder.decode(pdfArchiveUrl, StandardCharsets.UTF_8));
                processedFile.setPdfEmailFileUrl(URLDecoder.decode(pdfEmailUrl, StandardCharsets.UTF_8));
                processedFile.setHtmlEmailFileUrl(URLDecoder.decode(htmlEmailUrl, StandardCharsets.UTF_8));
                processedFile.setTxtEmailFileUrl(URLDecoder.decode(txtEmailUrl, StandardCharsets.UTF_8));
                processedFile.setPdfMobstatFileUrl(URLDecoder.decode(mobstatUrl, StandardCharsets.UTF_8));
                processedFile.setBlobURL(URLDecoder.decode(copiedTriggerUrl, StandardCharsets.UTF_8));
                processedFile.setStatusCode("OK");
                processedFile.setStatusDescription("Success");
                processedFiles.add(processedFile);
                fileCount++;
            }
        } catch (Exception ex) {
            logger.error("Error processing file '{}': {}", file.getFilename(), ex.getMessage(), ex);
        }
    }

    PrintFile printFile = new PrintFile();
    printFile.setPrintFileURL(blobStorageService.buildPrintFileUrl(message));
    printFiles.add(printFile);

    payload.setFileCount(processedFiles.size());

    metadata.setProcessingStatus("Completed");
    metadata.setTotalFilesProcessed(processedFiles.size());
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription("Success");

    SummaryPayload summaryPayload = new SummaryPayload();
    summaryPayload.setBatchID(message.getBatchId());
    summaryPayload.setFileName(fileName);
    summaryPayload.setHeader(header);
    summaryPayload.setMetadata(metadata);
    summaryPayload.setPayload(payload);
    summaryPayload.setProcessedFiles(processedFiles);
    summaryPayload.setPrintFiles(printFiles);

    // ✅ Write the summary JSON file
    String summaryJsonPath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload);
    String summaryFileName = "summary_" + message.getBatchId() + ".json";
    summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message, summaryFileName);
    String decodedUrl = URLDecoder.decode(summaryFileUrl, StandardCharsets.UTF_8);
    summaryPayload.setSummaryFileURL(decodedUrl);

    // 👇 ADDED FOR metadata.json generation and upload to Trigger folder
    try {
        Map<String, Object> metadataMap = objectMapper.convertValue(message, Map.class);
        if (metadataMap.containsKey("batchFiles")) {
            List<Map<String, Object>> files = (List<Map<String, Object>>) metadataMap.get("batchFiles");
            for (Map<String, Object> f : files) {
                Object blobUrl = f.remove("blobUrl");
                if (blobUrl != null) {
                    f.put("fileLocation", blobUrl);
                }
            }
        }

        String metadataJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metadataMap);

        // ✅ Save to Windows temp directory
        String localMetadataPath = System.getProperty("java.io.tmpdir") + "metadata_" + message.getBatchId() + ".json";
        File metadataFile = new File(localMetadataPath);
        org.apache.commons.io.FileUtils.writeStringToFile(metadataFile, metadataJson, StandardCharsets.UTF_8);

        String metadataTargetBlobPath = String.format("%s/%s/Trigger/metadata_%s.json",
                inputTopic, message.getSourceSystem(), message.getBatchId());

        blobStorageService.uploadFile(localMetadataPath, metadataTargetBlobPath);
        logger.info("✅ metadata.json uploaded to Trigger path: {}", metadataTargetBlobPath);
    } catch (Exception e) {
        logger.error("❌ Failed to write or upload metadata.json: {}", e.getMessage(), e);
    }
    // ☝️ END of metadata.json block

    SummaryResponse summaryResponse = new SummaryResponse();
    summaryResponse.setBatchID(summaryPayload.getBatchID());
    summaryResponse.setFileName(summaryPayload.getFileName());
    summaryResponse.setHeader(summaryPayload.getHeader());
    summaryResponse.setMetadata(summaryPayload.getMetadata());
    summaryResponse.setPayload(summaryPayload.getPayload());
    summaryResponse.setSummaryFileURL(summaryPayload.getSummaryFileURL());
    summaryResponse.setTimestamp(String.valueOf(Instant.now()));

    SummaryPayloadResponse apiPayload = new SummaryPayloadResponse("Batch processed successfully", "success", summaryResponse);
    return new ApiResponse(apiPayload.getMessage(), apiPayload.getStatus(), apiPayload.getSummaryResponse());
}
