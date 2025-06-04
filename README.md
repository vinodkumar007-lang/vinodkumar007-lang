private ApiResponse processSingleMessage(KafkaMessage message) {
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
    String summaryFileUrl;
    int fileCount = 0;

    // <-- HERE is the updated fileName extraction:
    String fileName = null;
    if (message.getBatchFiles() != null && !message.getBatchFiles().isEmpty()) {
        String firstBlobUrl = message.getBatchFiles().get(0).getBlobUrl();
        String blobPath = extractBlobPath(firstBlobUrl);
        fileName = extractFileName(blobPath);
    }
    if (fileName == null || fileName.isEmpty()) {
        fileName = message.getBatchId() + ".json";  // fallback if no file name found
    }

    for (BatchFile file : message.getBatchFiles()) {
        try {
            logger.debug("Processing batch file: {}", file.getFilename());

            String sourceBlobUrl = file.getBlobUrl();
            String resolvedBlobPath = extractBlobPath(sourceBlobUrl);
            String sanitizedBlobName = extractFileName(resolvedBlobPath);
            String inputFileContent = blobStorageService.downloadFileContent(sanitizedBlobName);

            logger.debug("Downloaded file content length for {}: {}", sanitizedBlobName,
                    inputFileContent != null ? inputFileContent.length() : "null");

            assert inputFileContent != null;
            List<CustomerData> customers = DataParser.extractCustomerData(inputFileContent, "PRINT");

            if (customers.isEmpty()) {
                logger.warn("No customers extracted from file {}", file.getFilename());
                continue;
            }

            logger.debug("Extracted {} customers from file {}", customers.size(), file.getFilename());

            for (CustomerData customer : customers) {
                logger.debug("Processing customer ID: {}", customer.getCustomerId());

                File pdfFile = FileGenerator.generatePdf(customer);
                File htmlFile = FileGenerator.generateHtml(customer);
                File txtFile = FileGenerator.generateTxt(customer);
                File mobstatFile = FileGenerator.generateMobstat(customer);

                String pdfArchiveBlobPath = buildBlobPath(
                        message.getSourceSystem(),
                        message.getTimestamp(),
                        message.getBatchId(),
                        message.getUniqueConsumerRef(),
                        message.getJobName(),
                        "archive",
                        customer.getAccountNumber(),
                        pdfFile.getName());

                String pdfEmailBlobPath = buildBlobPath(
                        message.getSourceSystem(),
                        message.getTimestamp(),
                        message.getBatchId(),
                        message.getUniqueConsumerRef(),
                        message.getJobName(),
                        "email",
                        customer.getAccountNumber(),
                        pdfFile.getName());

                String htmlEmailBlobPath = buildBlobPath(
                        message.getSourceSystem(),
                        message.getTimestamp(),
                        message.getBatchId(),
                        message.getUniqueConsumerRef(),
                        message.getJobName(),
                        "html",
                        customer.getAccountNumber(),
                        htmlFile.getName());

                String txtEmailBlobPath = buildBlobPath(
                        message.getSourceSystem(),
                        message.getTimestamp(),
                        message.getBatchId(),
                        message.getUniqueConsumerRef(),
                        message.getJobName(),
                        "txt",
                        customer.getAccountNumber(),
                        txtFile.getName());

                String mobstatBlobPath = buildBlobPath(
                        message.getSourceSystem(),
                        message.getTimestamp(),
                        message.getBatchId(),
                        message.getUniqueConsumerRef(),
                        message.getJobName(),
                        "mobstat",
                        customer.getAccountNumber(),
                        mobstatFile.getName());

                String pdfArchiveUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(), pdfArchiveBlobPath);
                String pdfEmailUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(), pdfEmailBlobPath);
                String htmlEmailUrl = blobStorageService.uploadFile(htmlFile.getAbsolutePath(), htmlEmailBlobPath);
                String txtEmailUrl = blobStorageService.uploadFile(txtFile.getAbsolutePath(), txtEmailBlobPath);
                String mobstatUrl = blobStorageService.uploadFile(mobstatFile.getAbsolutePath(), mobstatBlobPath);

                if (pdfArchiveUrl == null || pdfArchiveUrl.isBlank()) {
                    logger.warn("pdfArchiveUrl is empty for customerId: {}", customer.getCustomerId());
                }
                if (pdfEmailUrl == null || pdfEmailUrl.isBlank()) {
                    logger.warn("pdfEmailUrl is empty for customerId: {}", customer.getCustomerId());
                }
                if (htmlEmailUrl == null || htmlEmailUrl.isBlank()) {
                    logger.warn("htmlEmailUrl is empty for customerId: {}", customer.getCustomerId());
                }
                if (txtEmailUrl == null || txtEmailUrl.isBlank()) {
                    logger.warn("txtEmailUrl is empty for customerId: {}", customer.getCustomerId());
                }
                if (mobstatUrl == null || mobstatUrl.isBlank()) {
                    logger.warn("mobstatUrl is empty for customerId: {}", customer.getCustomerId());
                }

                SummaryProcessedFile processedFile = new SummaryProcessedFile();
                processedFile.setCustomerID(customer.getCustomerId());
                processedFile.setAccountNumber(customer.getAccountNumber());
                processedFile.setPdfArchiveFileURL(pdfArchiveUrl);
                processedFile.setPdfEmailFileURL(pdfEmailUrl);
                processedFile.setHtmlEmailFileURL(htmlEmailUrl);
                processedFile.setTxtEmailFileURL(txtEmailUrl);
                processedFile.setPdfMobstatFileURL(mobstatUrl);
                processedFile.setStatusCode("OK");
                processedFile.setStatusDescription("Success");

                processedFiles.add(processedFile);
                fileCount++;
                logger.debug("Added processed file for customer ID: {}", customer.getCustomerId());
            }
        } catch (Exception ex) {
            logger.error("Error processing file '{}': {}", file.getFilename(), ex.getMessage(), ex);
        }
    }

    logger.info("Total processed files count: {}", processedFiles.size());

    PrintFile printFile = new PrintFile();
    printFile.setPrintFileURL(blobStorageService.buildPrintFileUrl(message));
    printFiles.add(printFile);

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

    String summaryJsonPath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload);
    logger.debug("Summary JSON file path: {}", summaryJsonPath);
    summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message);
    summaryPayload.setSummaryFileURL(summaryFileUrl);

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
