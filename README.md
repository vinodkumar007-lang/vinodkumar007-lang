private SummaryPayload processMessages(List<String> allMessages) throws IOException {
    List<CustomerSummary> customerSummaries = new ArrayList<>();
    String fileName = "";
    String jobName = "";
    String batchId = null;

    JsonNode firstRoot = null;
    if (!allMessages.isEmpty()) {
        firstRoot = objectMapper.readTree(allMessages.get(0));
        // Try to get batchId and jobName from first message root node
        batchId = safeGetText(firstRoot, "BatchId", true);
        jobName = safeGetText(firstRoot, "JobName", false);
    }

    for (String message : allMessages) {
        JsonNode root = objectMapper.readTree(message);

        JsonNode batchFilesNode = root.has("BatchFiles") ? root.get("BatchFiles") : root.get("batchFiles");
        if (batchFilesNode == null || !batchFilesNode.isArray()) {
            logger.warn("No BatchFiles found");
            continue;
        }

        for (JsonNode fileNode : batchFilesNode) {
            String filePath = safeGetText(fileNode, "BlobUrl", false);
            String objectId = safeGetText(fileNode, "ObjectId", false);
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
    }

    HeaderInfo headerInfo = buildHeader(firstRoot, jobName);

    SummaryPayload summaryPayload = new SummaryPayload();
    summaryPayload.setHeader(headerInfo);

    PayloadInfo payloadInfo = new PayloadInfo();
    // DON'T add processedFiles if you don't want it in response
    payloadInfo.setPrintFiles(Collections.emptyList()); // keep printFiles if needed
    summaryPayload.setPayload(payloadInfo);

    MetaDataInfo metaDataInfo = new MetaDataInfo();
    metaDataInfo.setCustomerSummaries(customerSummaries);
    summaryPayload.setMetadata(metaDataInfo);

    return summaryPayload;
}

private HeaderInfo buildHeader(JsonNode root, String jobName) {
    HeaderInfo headerInfo = new HeaderInfo();
    if (root != null) {
        headerInfo.setBatchId(safeGetText(root, "BatchId", true));
        headerInfo.setTenantCode(safeGetText(root, "TenantCode", false));
        headerInfo.setChannelID(safeGetText(root, "ChannelID", false));
        headerInfo.setAudienceID(safeGetText(root, "AudienceID", false));
        headerInfo.setSourceSystem(safeGetText(root, "SourceSystem", false));
        headerInfo.setProduct(safeGetText(root, "Product", false));
    }
    headerInfo.setJobName(jobName != null ? jobName : "");
    headerInfo.setTimestamp(new Date().toString());
    return headerInfo;
}

private String safeGetText(JsonNode node, String fieldName, boolean required) {
    if (node != null && node.has(fieldName) && !node.get(fieldName).isNull()) {
        String value = node.get(fieldName).asText();
        return value.equalsIgnoreCase("null") ? "" : value;
    }
    return required ? "" : null;
}
