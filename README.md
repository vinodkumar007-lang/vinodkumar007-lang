public String uploadFile(String content, String targetPath) {
        try {
            initSecrets();
            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(containerName).getBlobClient(targetPath);
            blob.upload(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), content.length(), true);

            logger.info("Uploaded file to '{}'", blob.getBlobUrl());
            return blob.getBlobUrl();
        } catch (Exception e) {
            logger.error("Upload failed: {}", e.getMessage(), e);
            throw new CustomAppException("Upload failed", 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private List<SummaryProcessedFile> buildAndUploadProcessedFiles(Path jobDir, Map<String, String> accountCustomerMap, KafkaMessage msg) throws IOException {
        List<SummaryProcessedFile> list = new ArrayList<>();
        List<String> folders = List.of("archive", "email", "html", "mobstat", "txt");

        for (String folder : folders) {
            Path subDir = jobDir.resolve(folder);
            if (!Files.exists(subDir)) continue;

            Files.list(subDir).forEach(file -> {
                try {
                    String fileName = file.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
                    if (account == null) return;
                    String customer = accountCustomerMap.get(account);
                    SummaryProcessedFile entry = list.stream().filter(e -> account.equals(e.getAccountNumber())).findFirst().orElseGet(() -> {
                        SummaryProcessedFile newEntry = new SummaryProcessedFile();
                        newEntry.setAccountNumber(account);
                        newEntry.setCustomerId(customer);
                        newEntry.setStatusCode("OK");
                        newEntry.setStatusDescription("Success");
                        list.add(newEntry);
                        return newEntry;
                    });
                    String blobUrl = blobStorageService.uploadFile(Files.readString(file), String.format("%s/%s/%s/%s/%s",
                            msg.getSourceSystem(), msg.getBatchId(), msg.getUniqueConsumerRef(), folder, fileName));
                    switch (folder) {
                        case "archive" -> entry.setPdfArchiveFileUrl(blobUrl);
                        case "email" -> entry.setPdfEmailFileUrl(blobUrl);
                        case "html" -> entry.setHtmlEmailFileUrl(blobUrl);
                        case "txt" -> entry.setTxtEmailFileUrl(blobUrl);
                        case "mobstat" -> entry.setPdfMobstatFileUrl(blobUrl);
                    }
                } catch (Exception e) {
                    logger.error("Error uploading file: {}", file, e);
                }
            });
        }
        return list;
    }
