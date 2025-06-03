private ApiResponse processSingleMessage(KafkaMessage message) {
        if (message == null) {
            return new ApiResponse("Empty message", "error", null);
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
        String summaryFileUrl = null;
        int fileCount = 0;

        String fileName = message.getBatchId() + ".json";

        for (BatchFile file : message.getBatchFiles()) {
            try {
                String sourceBlobUrl = file.getBlobUrl();
                String inputFileName = file.getFilename();
                if (inputFileName != null && !inputFileName.isBlank()) {
                    fileName = inputFileName;
                }

                String inputFileContent = blobStorageService.downloadFileContent(sourceBlobUrl);

                List<CustomerData> customers = DataParser.extractCustomerData(inputFileContent);

                for (CustomerData customer : customers) {
                    File pdfFile = FileGenerator.generatePdf(customer);
                    File htmlFile = FileGenerator.generateHtml(customer);
                    File txtFile = FileGenerator.generateTxt(customer);
                    File mobstatFile = FileGenerator.generateMobstat(customer);

                    String pdfArchiveBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "archive", customer.getAccountNumber(), pdfFile.getName());

                    String pdfEmailBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "email", customer.getAccountNumber(), pdfFile.getName());

                    String htmlEmailBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "html", customer.getAccountNumber(), htmlFile.getName());

                    String txtEmailBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "txt", customer.getAccountNumber(), txtFile.getName());

                    String mobstatBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "mobstat", customer.getAccountNumber(), mobstatFile.getName());

                    String pdfArchiveUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(), pdfArchiveBlobPath);
                    String pdfEmailUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(), pdfEmailBlobPath);
                    String htmlEmailUrl = blobStorageService.uploadFile(htmlFile.getAbsolutePath(), htmlEmailBlobPath);
                    String txtEmailUrl = blobStorageService.uploadFile(txtFile.getAbsolutePath(), txtEmailBlobPath);
                    String mobstatUrl = blobStorageService.uploadFile(mobstatFile.getAbsolutePath(), mobstatBlobPath);

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
                }
            } catch (Exception ex) {
                logger.warn("Error processing file '{}': {}", file.getFilename(), ex.getMessage());
            }
        }

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL(buildPrintFileUrl(message));
        printFiles.add(printFile);

        metadata.setProcessedFileList(processedFiles);
        metadata.setPrintFile(printFiles);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName(fileName);
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);
        summaryPayload.setPayload(payload);

        String summaryFilePath = SummaryJsonWriter.writeSummaryJson(summaryPayload);
        File summaryJsonFile = new File(summaryFilePath);

        try {
            String jsonContent = new String(java.nio.file.Files.readAllBytes(summaryJsonFile.toPath()));
            logger.info("📄 Summary JSON content before upload:\n{}", jsonContent);
        } catch (IOException e) {
            logger.warn("⚠️ Could not read summary.json for logging", e);
        }

        summaryFileUrl = blobStorageService.uploadFile(summaryFilePath, buildSummaryJsonBlobPath(message));
        summaryPayload.setSummaryFileURL(summaryFileUrl);
        payload.setFileCount(fileCount);

        SummaryPayloadResponse apiPayload = new SummaryPayloadResponse();
        apiPayload.setBatchID(summaryPayload.getBatchID());
        apiPayload.setFileName(summaryPayload.getFileName());
        apiPayload.setHeader(summaryPayload.getHeader());
        apiPayload.setMetadata(summaryPayload.getMetadata());
        apiPayload.setPayload(summaryPayload.getPayload());
        apiPayload.setSummaryFileURL(summaryFileUrl);
        apiPayload.setTimestamp(Instant.now().toString());

        return new ApiResponse("Batch processed successfully", "success", apiPayload);
    }
