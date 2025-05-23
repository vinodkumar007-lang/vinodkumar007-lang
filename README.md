private SummaryPayload processMessages(List<String> allMessages) throws IOException {
    List<CustomerSummary> customerSummaries = new ArrayList<>();
    String fileName = "";
    String jobName = "";
    String batchId = null;

    // Define cutoff: skip messages older than 1 day
    Instant cutoffTime = Instant.now().minus(Duration.ofDays(1));

    for (String message : allMessages) {
        JsonNode root = objectMapper.readTree(message);

        // 1. Skip messages older than cutoff based on "Timestamp" field
        String timestampStr = safeGetText(root, "Timestamp", false);
        if (timestampStr != null) {
            try {
                Instant messageTime = Instant.parse(timestampStr);
                if (messageTime.isBefore(cutoffTime)) {
                    logger.info("Skipping old message with timestamp: {}", timestampStr);
                    continue;
                }
            } catch (Exception e) {
                logger.warn("Invalid timestamp format: {}", timestampStr);
                // optionally skip or continue processing
            }
        }

        // 2. Set batchId if not already
        if (batchId == null) {
            batchId = safeGetText(root, "BatchId", false);
        }

        JsonNode batchFilesNode = root.get("BatchFiles");
        if (batchFilesNode == null || !batchFilesNode.isArray()) {
            logger.warn("No BatchFiles found");
            continue;
        }

        for (JsonNode fileNode : batchFilesNode) {
            String filePath = safeGetText(fileNode, "BlobUrl", false);
            String objectId = safeGetText(fileNode, "ObjectId", false);
            String localFileName = safeGetText(fileNode, "Filename", false);
            String validationStatus = safeGetText(fileNode, "ValidationStatus", false);

            if (filePath == null || objectId == null) continue;

            try {
                blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
            } catch (Exception e) {
                logger.warn("Blob URL creation failed for {}", filePath, e);
            }

            String extension = getFileExtension(filePath);
            String customerId = objectId;

            CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
            detail.setObjectId(objectId);
            detail.setFileLocation(filePath);
            detail.setFileUrl("https://" + azureBlobStorageAccount + "/" + filePath);
            detail.setEncrypted(isEncrypted(filePath, extension));
            detail.setStatus(validationStatus != null ? validationStatus : "OK");
            detail.setType(determineType(filePath, extension));

            CustomerSummary customer = customerSummaries.stream()
                    .filter(c -> c.getCustomerId().equals(customerId))
                    .findFirst()
                    .orElseGet(() -> {
                        CustomerSummary c = new CustomerSummary();
                        c.setCustomerId(customerId);
                        c.setAccountNumber("");
                        c.setFiles(new ArrayList<>());
                        customerSummaries.add(c);
                        return c;
                    });

            customer.getFiles().add(detail);
        }

        if (jobName == null || jobName.isEmpty()) {
            jobName = safeGetText(root, "JobName", false);
        }

        if (fileName == null || fileName.isEmpty()) {
            JsonNode firstFileNode = batchFilesNode.get(0);
            fileName = safeGetText(firstFileNode, "Filename", false);
        }
    }

    HeaderInfo headerInfo = buildHeader(allMessages.get(0), jobName);

    List<Map<String, Object>> processedFiles = new ArrayList<>();
    for (CustomerSummary customer : customerSummaries) {
        Map<String, Object> pf = new HashMap<>();
        pf.put("customerID", customer.getCustomerId());
        pf.put("accountNumber", customer.getAccountNumber());
        for (CustomerSummary.FileDetail detail : customer.getFiles()) {
            pf.put(detail.getType() + "FileURL", detail.getFileUrl());
        }
        pf.put("statusCode", "OK");
        pf.put("statusDescription", "Success");
        processedFiles.add(pf);
    }

    SummaryPayload summaryPayload = new SummaryPayload();
    summaryPayload.setHeader(headerInfo);

    PayloadInfo payloadInfo = new PayloadInfo();
    payloadInfo.setProcessedFiles(processedFiles);
    payloadInfo.setPrintFiles(Collections.emptyList());
    summaryPayload.setPayload(payloadInfo);

    MetaDataInfo metaDataInfo = new MetaDataInfo();
    metaDataInfo.setCustomerSummaries(customerSummaries);
    summaryPayload.setMetadata(metaDataInfo);

    return summaryPayload;
}
