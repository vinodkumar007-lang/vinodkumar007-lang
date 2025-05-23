private SummaryPayload processMessages(List<String> allMessages) throws IOException {
    List<CustomerSummary> customerSummaries = new ArrayList<>();
    String fileName = "";
    String jobName = "";
    String batchId = null;

    Set<String> archived = new HashSet<>();
    Set<String> emailed = new HashSet<>();
    Set<String> mobstat = new HashSet<>();
    Set<String> printed = new HashSet<>();

    for (String message : allMessages) {
        JsonNode root = objectMapper.readTree(message);
        if (batchId == null) {
            batchId = safeGetText(root, "consumerReference", false);
        }

        JsonNode batchFilesNode = root.get("BatchFiles");
        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            logger.warn("No BatchFiles found in message: {}", message);
            continue; // skip this message instead of throwing
        }

        for (JsonNode fileNode : batchFilesNode) {
            String filePath = safeGetText(fileNode, "fileLocation", false);
            String objectId = safeGetText(fileNode, "ObjectId", false);

            if (filePath == null || objectId == null) {
                logger.warn("Missing fileLocation or ObjectId in BatchFile: {}", fileNode.toString());
                continue; // skip malformed file
            }

            try {
                blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
            } catch (Exception e) {
                logger.warn("Failed to generate SAS URL for file {}", filePath, e);
            }

            String extension = getFileExtension(filePath).toLowerCase();
            String customerId = objectId.split("_")[0];

            if (fileNode.has("fileName")) {
                fileName = safeGetText(fileNode, "fileName", false);
            }

            if (fileNode.has("jobName")) {
                jobName = safeGetText(fileNode, "jobName", false);
            }

            CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
            detail.setObjectId(objectId);
            detail.setFileLocation(filePath);
            detail.setFileUrl("https://" + azureBlobStorageAccount + "/" + filePath);
            detail.setStatus(extension.equals(".ps") ? "failed" : "OK");
            detail.setEncrypted(isEncrypted(filePath, extension));
            detail.setType(determineType(filePath, extension));

            if (filePath.contains("mobstat")) mobstat.add(customerId);
            if (filePath.contains("archive")) archived.add(customerId);
            if (filePath.contains("email")) emailed.add(customerId);
            if (extension.equals(".ps")) printed.add(customerId);

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

    List<Map<String, Object>> processedFiles = new ArrayList<>();
    for (CustomerSummary customer : customerSummaries) {
        Map<String, Object> pf = new HashMap<>();
        pf.put("customerID", customer.getCustomerId());
        pf.put("accountNumber", customer.getAccountNumber());

        for (CustomerSummary.FileDetail detail : customer.getFiles()) {
            String key = switch (detail.getType()) {
                case "pdf_archive" -> "pdfArchiveFileURL";
                case "pdf_email" -> "pdfEmailFileURL";
                case "html_email" -> "htmlEmailFileURL";
                case "txt_email" -> "txtEmailFileURL";
                case "pdf_mobstat" -> "pdfMobstatFileURL";
                default -> null;
            };
            if (key != null) {
                pf.put(key, detail.getFileUrl());
            }
        }

        pf.put("statusCode", "OK");
        pf.put("statusDescription", "Success");
        processedFiles.add(pf);
    }

    List<Map<String, Object>> printFiles = new ArrayList<>();
    for (CustomerSummary customer : customerSummaries) {
        for (CustomerSummary.FileDetail detail : customer.getFiles()) {
            if ("ps_print".equals(detail.getType())) {
                Map<String, Object> pf = new HashMap<>();
                pf.put("printFileURL", "https://" + azureBlobStorageAccount + "/pdfs/mobstat/" + detail.getObjectId());
                printFiles.add(pf);
            }
        }
    }

    HeaderInfo headerInfo = null;
    if (!allMessages.isEmpty()) {
        JsonNode firstRoot = objectMapper.readTree(allMessages.get(0));
        headerInfo = buildHeader(firstRoot, jobName);
    } else {
        headerInfo = new HeaderInfo();
    }

    PayloadInfo payloadInfo = new PayloadInfo();
    payloadInfo.setProcessedFiles(processedFiles);
    payloadInfo.setPrintFiles(printFiles);

    MetaDataInfo metaDataInfo = new MetaDataInfo();
    metaDataInfo.setCustomerSummaries(customerSummaries);

    SummaryPayload summaryPayload = new SummaryPayload();
    summaryPayload.setHeader(headerInfo);
    summaryPayload.setPayload(payloadInfo);
    summaryPayload.setMetadata(metaDataInfo);

    return summaryPayload;
}
