for (BatchFile file : message.getBatchFiles()) {
    try {
        // ✅ Copy the blob to Trigger folder
        String originalBlobUrl = file.getBlobUrl();
        String originalFileName = extractFileName(extractBlobPath(originalBlobUrl));
        String triggerBlobPath = String.format("%s/Trigger/%s", message.getSourceSystem(), originalFileName);
        String copiedTriggerUrl = blobStorageService.copyFileFromUrlToBlob(originalBlobUrl, triggerBlobPath);

        // ✅ Read content from original blob file (unchanged)
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
            processedFile.setPdfArchiveFileUrl(pdfArchiveUrl);
            processedFile.setPdfEmailFileUrl(pdfEmailUrl);
            processedFile.setHtmlEmailFileUrl(htmlEmailUrl);
            processedFile.setTxtEmailFileUrl(txtEmailUrl);
            processedFile.setPdfMobstatFileUrl(mobstatUrl);

            // ✅ Set copied trigger URL
            processedFile.setTriggerFileUrl(copiedTriggerUrl);

            processedFile.setStatusCode("OK");
            processedFile.setStatusDescription("Success");
            processedFiles.add(processedFile);
            fileCount++;
        }
    } catch (Exception ex) {
        logger.error("Error processing file '{}': {}", file.getFilename(), ex.getMessage(), ex);
    }
}
