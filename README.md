 // Append to summary.json on disk
            logger.info("Appending payloads to summary.json file...");
            SummaryJsonWriter.appendToSummaryJson(summaryFile, processedPayloads, azureBlobStorageAccount);

            // Upload summary.json to blob storage and get URL
            logger.info("Uploading summary.json to blob storage...");
            String summaryFileUrl = blobStorageService.uploadSummaryJson(summaryFile);
            logger.info("Summary file uploaded to: {}", summaryFileUrl);


 public String uploadSummaryJson(
            String sourceSystem,
            String batchId,
            String timestamp,
            String summaryJsonContent) {

        try {
            if (summaryJsonContent == null || sourceSystem == null || batchId == null || timestamp == null) {
                throw new CustomAppException("Required parameters missing for summary upload", 400, HttpStatus.BAD_REQUEST);
            }

            initSecrets();

            String blobName = String.format("%s/summary/%s/%s/summary.json",
                    sourceSystem,
                    timestamp,
                    batchId);

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient summaryBlobClient = containerClient.getBlobClient(blobName);

            byte[] jsonBytes = summaryJsonContent.getBytes(StandardCharsets.UTF_8);

            summaryBlobClient.upload(new java.io.ByteArrayInputStream(jsonBytes), jsonBytes.length, true);

            logger.info("✅ Uploaded summary.json to '{}'", summaryBlobClient.getBlobUrl());

            return summaryBlobClient.getBlobUrl();

        } catch (CustomAppException cae) {
            throw cae;
        } catch (Exception e) {
            logger.error("❌ Error uploading summary.json: {}", e.getMessage(), e);
            throw new CustomAppException("Error uploading summary.json", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public synchronized static void appendToSummaryJson(File summaryFile, SummaryPayload summaryPayload) throws IOException {
        JSONObject rootJson;
        if (summaryFile.exists()) {
            String existingContent = readFileContent(summaryFile);
            if (existingContent.isEmpty()) {
                rootJson = new JSONObject();
            } else {
                rootJson = new JSONObject(existingContent);
            }
        } else {
            rootJson = new JSONObject();
        }

        // header & metadata
        JSONObject headerJson = new JSONObject(summaryPayload.getHeader());
        JSONObject metadataJson = new JSONObject(summaryPayload.getMetadata());
        JSONObject payloadJson = new JSONObject(summaryPayload.getPayload());

        rootJson.put("header", headerJson);
        rootJson.put("metadata", metadataJson);
        rootJson.put("payload", payloadJson);

        // processedFiles array
        JSONArray processedFilesArray = rootJson.optJSONArray("processedFiles");
        if (processedFilesArray == null) processedFilesArray = new JSONArray();

        // Append unique processedFiles
        for (var pf : summaryPayload.getProcessedFiles()) {
            String uniqueKey = pf.getSourceURL() + pf.getFileName();
            if (!processedFileKeys.contains(uniqueKey)) {
                processedFileKeys.add(uniqueKey);
                JSONObject pfJson = new JSONObject();
                pfJson.put("sourceURL", pf.getSourceURL());
                pfJson.put("fileName", pf.getFileName());
                pfJson.put("status", pf.getStatus());
                pfJson.put("blobURL", pf.getBlobURL());
                pfJson.put("batchId", pf.getBatchId());
                processedFilesArray.put(pfJson);
            }
        }
        rootJson.put("processedFiles", processedFilesArray);

        // printFiles array
        JSONArray printFilesArray = rootJson.optJSONArray("printFiles");
        if (printFilesArray == null) printFilesArray = new JSONArray();

        for (var pf : summaryPayload.getPrintFiles()) {
            if (!printFileUrls.contains(pf.getPrintFileURL())) {
                printFileUrls.add(pf.getPrintFileURL());
                JSONObject pfJson = new JSONObject();
                pfJson.put("printFileURL", pf.getPrintFileURL());
                printFilesArray.put(pfJson);
            }
        }
        rootJson.put("printFiles", printFilesArray);

        // Save back to file
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(summaryFile), StandardCharsets.UTF_8))) {
            writer.write(rootJson.toString(4));
        }
    }
