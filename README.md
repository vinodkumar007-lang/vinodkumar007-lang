private SummaryPayload processSingleMessage(String message) throws IOException {
    JsonNode root = objectMapper.readTree(message);

    // ✅ Extract sourceSystem dynamically from root or Payload (fallback to DEBTMAN)
    String sourceSystem = safeGetText(root, "sourceSystem", false);
    if (sourceSystem == null) {
        JsonNode payloadNode = root.get("Payload");
        if (payloadNode != null) {
            sourceSystem = safeGetText(payloadNode, "sourceSystem", false);
        }
    }
    if (sourceSystem == null || sourceSystem.isBlank()) {
        sourceSystem = "DEBTMAN";
    }

    // Extract jobName (optional)
    String jobName = safeGetText(root, "JobName", false);
    if (jobName == null) jobName = "";

    // Extract BatchId (mandatory fallback to random UUID)
    String batchId = safeGetText(root, "BatchId", true);
    if (batchId == null) batchId = UUID.randomUUID().toString();

    // Extract timestamp (use current timestamp if not in message)
    String timestamp = Instant.now().toString();

    // Extract consumerReference
    String consumerReference = safeGetText(root, "consumerReference", false);
    JsonNode payloadNode = root.get("Payload");
    if (consumerReference == null && payloadNode != null) {
        consumerReference = safeGetText(payloadNode, "consumerReference", false);
    }
    if (consumerReference == null) consumerReference = "unknownConsumer";

    // Extract processReference (we'll use eventID as fallback)
    String processReference = safeGetText(root, "eventID", false);
    if (processReference == null && payloadNode != null) {
        processReference = safeGetText(payloadNode, "eventID", false);
    }
    if (processReference == null) processReference = "unknownProcess";

    // Process CustomerSummaries from BatchFiles array
    List<CustomerSummary> customerSummaries = new ArrayList<>();
    JsonNode batchFilesNode = root.get("BatchFiles");
    if (batchFilesNode != null && batchFilesNode.isArray()) {
        for (JsonNode fileNode : batchFilesNode) {
            String filePath = safeGetText(fileNode, "BlobUrl", true);
            String objectId = safeGetText(fileNode, "ObjectId", true);
            String validationStatus = safeGetText(fileNode, "ValidationStatus", false);

            if (filePath == null || objectId == null) {
                logger.warn("Skipping file with missing BlobUrl or ObjectId.");
                continue;
            }

            try {
                fileLocation = blobStorageService.uploadFileAndReturnLocation(
                    sourceSystem,
                    filePath,
                    batchId,
                    objectId,
                    consumerReference,
                    processReference,
                    timestamp
                );
            } catch (Exception e) {
                logger.warn("Blob upload failed for {}: {}", filePath, e.getMessage());
            }

            String extension = getFileExtension(filePath);
            CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
            detail.setObjectId(objectId);
            detail.setFileLocation(filePath);
            detail.setFileUrl("https://" + azureBlobStorageAccount + "/" + filePath);
            detail.setEncrypted(isEncrypted(filePath, extension));
            detail.setStatus(validationStatus != null ? validationStatus : "OK");
            detail.setType(determineType(filePath));

            CustomerSummary customer = customerSummaries.stream()
                    .filter(c -> c.getCustomerId().equals(objectId))
                    .findFirst()
                    .orElseGet(() -> {
                        CustomerSummary c = new CustomerSummary();
                        c.setCustomerId(objectId);
                        c.setAccountNumber("");
                        c.setFiles(new ArrayList<>());
                        customerSummaries.add(c);
                        return c;
                    });

            customer.getFiles().add(detail);
        }
    }

    // Build Header info
    HeaderInfo headerInfo;
    JsonNode headerNode = root.get("Header");
    if (headerNode != null && !headerNode.isNull()) {
        headerInfo = buildHeader(headerNode, jobName);
    } else {
        headerInfo = buildHeader(root, jobName);
    }
    if (headerInfo.getBatchId() == null) {
        headerInfo.setBatchId(batchId);
    }

    // Extract product field if present
    String product = null;
    if (headerNode != null && headerNode.has("product")) {
        product = safeGetText(headerNode, "product", false);
    }
    if (product == null) {
        product = safeGetText(root, "product", false);
    }
    headerInfo.setProduct(product);

    // Build Payload info
    PayloadInfo payloadInfo = new PayloadInfo();
    if (payloadNode != null && !payloadNode.isNull()) {
        payloadInfo.setUniqueConsumerRef(safeGetText(payloadNode, "uniqueConsumerRef", false));
        payloadInfo.setUniqueECPBatchRef(safeGetText(payloadNode, "uniqueECPBatchRef", false));
        payloadInfo.setRunPriority(safeGetText(payloadNode, "runPriority", false));
        payloadInfo.setEventID(safeGetText(payloadNode, "eventID", false));
        payloadInfo.setEventType(safeGetText(payloadNode, "eventType", false));
        payloadInfo.setRestartKey(safeGetText(payloadNode, "restartKey", false));
        payloadInfo.setBlobURL(safeGetText(payloadNode, "blobURL", false));
        payloadInfo.setEventOutcomeCode(safeGetText(payloadNode, "eventOutcomeCode", false));
        payloadInfo.setEventOutcomeDescription(safeGetText(payloadNode, "eventOutcomeDescription", false));

        JsonNode printFilesNode = payloadNode.get("printFiles");
        if (printFilesNode != null && printFilesNode.isArray()) {
            List<String> printFiles = new ArrayList<>();
            for (JsonNode pf : printFilesNode) {
                printFiles.add(pf.asText());
            }
            payloadInfo.setPrintFiles(printFiles);
        }
    }

    MetaDataInfo metaDataInfo = new MetaDataInfo();
    metaDataInfo.setTotalFiles(customerSummaries.stream().mapToInt(c -> c.getFiles().size()).sum());
    metaDataInfo.setTotalCustomers(customerSummaries.size());

    SummaryPayload summaryPayload = new SummaryPayload();
    summaryPayload.setJobName(jobName);
    summaryPayload.setBatchId(batchId);
    summaryPayload.setCustomerSummary(customerSummaries);
    summaryPayload.setHeader(headerInfo);
    summaryPayload.setPayload(payloadInfo);
    summaryPayload.setMetaData(metaDataInfo);

    return summaryPayload;
}
