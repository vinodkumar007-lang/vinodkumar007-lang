 private ApiResponse processSingleMessage(KafkaMessage message) {
        logger.info("Processing batchId={}, sourceSystem={}", message.getBatchId(), message.getSourceSystem());

        List<SummaryProcessedFile> processedFiles = new ArrayList<>();
        List<PrintFile> printFiles = new ArrayList<>();

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

        Metadata metadata = new Metadata();
        int totalFilesProcessed = 0;

        for (BatchFile batchFile : message.getBatchFiles()) {
            String targetBlobPath = buildTargetBlobPath(
                    message.getSourceSystem(),
                    message.getTimestamp(),
                    message.getBatchId(),
                    message.getUniqueConsumerRef(),
                    message.getJobName(),
                    batchFile.getFilename()
            );

            logger.info("Copying file from '{}' to '{}'", batchFile.getBlobUrl(), targetBlobPath);
            String newBlobUrl = blobStorageService.copyFileFromUrlToBlob(batchFile.getBlobUrl(), targetBlobPath);

            SummaryProcessedFile spf = new SummaryProcessedFile();
            spf.setCustomerID("C001");
            spf.setAccountNumber("123456780123456");
            spf.setPdfArchiveFileURL(generatePdfUrl("archive", "123456780123456", message.getBatchId()));
            spf.setPdfEmailFileURL(generatePdfUrl("email", "123456780123456", message.getBatchId()));
            spf.setHtmlEmailFileURL(generatePdfUrl("html", "123456780123456", message.getBatchId()));
            spf.setTxtEmailFileURL(generatePdfUrl("txt", "123456780123456", message.getBatchId()));
            spf.setPdfMobstatFileURL(generatePdfUrl("mobstat", "123456780123456", message.getBatchId()));
            spf.setStatusCode("OK");
            spf.setStatusDescription("Success");
            processedFiles.add(spf);

            totalFilesProcessed++;
        }

        PrintFile pf = new PrintFile();
        pf.setPrintFileURL("https://" + azureBlobStorageAccount + "/pdfs/mobstat/PrintFileName1_" + message.getBatchId() + ".ps");
        printFiles.add(pf);

        metadata.setTotalFilesProcessed(totalFilesProcessed);
        metadata.setProcessingStatus("Completed");
        metadata.setEventOutcomeCode("200");
        metadata.setEventOutcomeDescription("Batch processed successfully");

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName("summary_" + message.getBatchId() + ".json");
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);
        summaryPayload.setPayload(payload);
        summaryPayload.setProcessedFiles(processedFiles);
        summaryPayload.setPrintFiles(printFiles);

        // Write summary JSON locally
        File summaryFile = new File(System.getProperty("java.io.tmpdir"), "summary.json");
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, summaryPayload);
            logger.info("Summary JSON written locally at {}", summaryFile.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to write summary.json", e);
            throw new RuntimeException(e);
        }

        // Upload summary.json to blob storage
        String summaryBlobPath = String.format("%s/summary/%s/summary.json",
                message.getSourceSystem(), message.getBatchId());
        String summaryFileUrl = blobStorageService.uploadFile(String.valueOf(summaryFile), summaryBlobPath);
        summaryPayload.setSummaryFileURL(summaryFileUrl);

        ApiResponse apiResponse = new ApiResponse();
        apiResponse.setMessage("Batch processed successfully");
        apiResponse.setStatus("success");
        apiResponse.setSummaryPayload(summaryPayload);

        return apiResponse;
    }
