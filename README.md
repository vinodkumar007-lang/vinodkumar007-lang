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

        // Extract batchId with fallback for naming variations
        if (batchId == null) {
            batchId = safeGetText(root, "consumerReference", false);
            if (batchId == null) {
                batchId = safeGetText(root, "BatchId", false);
            }
            if (batchId == null) {
                batchId = safeGetText(root, "UniqueConsumerRef", false);
            }
        }

        // Extract jobName with fallback for naming variations
        if (jobName == null || jobName.isEmpty()) {
            jobName = safeGetText(root, "jobName", false);
            if (jobName == null || jobName.isEmpty()) {
                jobName = safeGetText(root, "JobName", false);
            }
        }

        JsonNode batchFilesNode = root.get("BatchFiles");
        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            logger.warn("No BatchFiles found in message: {}", message);
            continue;
        }

        // Extract fileName from first batch file if not yet set
        if (fileName == null || fileName.isEmpty()) {
            JsonNode firstFile = batchFilesNode.get(0);
            fileName = safeGetText(firstFile, "fileName", false);
            if (fileName == null || fileName.isEmpty()) {
                fileName = safeGetText(firstFile, "Filename", false);
            }
        }

        for (JsonNode fileNode : batchFilesNode) {
            String filePath = safeGetText(fileNode, "fileLocation", false);
            if (filePath == null) {
                filePath = safeGetText(fileNode, "BlobUrl", false);
            }
            String objectId = safeGetText(fileNode, "ObjectId", false);

            if (filePath == null || objectId == null) {
                logger.warn("Missing fileLocation/ObjectId or BlobUrl/ObjectId in BatchFile: {}", fileNode.toString());
                continue;
            }

            try {
                blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
            } catch (Exception e) {
                logger.warn("Failed to generate SAS URL for file {}", filePath, e);
            }

            String extension = getFileExtension(filePath).toLowerCase();
            String customerId = objectId.split("_")[0];

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

    HeaderInfo headerInfo;
    if (!allMessages.isEmpty()) {
        JsonNode firstRoot = objectMapper.readTree(allMessages.get(0));
        headerInfo = buildHeader(firstRoot, jobName);
        // override batchId to ensure correctness
        headerInfo.setBatchId(batchId);
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

    // Set fileName and batchId in the payload for reference
    // (You may need to add setters if missing in SummaryPayload or HeaderInfo)
    summaryPayload.getHeader().setJobName(jobName != null ? jobName : "");
    // batchId already set on header above

    return summaryPayload;
}

// Updated method that writes the summary with correct batchId and fileName
private File writeSummaryToFile(SummaryPayload summaryPayload) throws IOException {
    String userHome = System.getProperty("user.home");
    File jsonFile = new File(userHome, "summary.json");

    Map<String, Object> summaryData = new HashMap<>();
    summaryData.put("batchID", summaryPayload.getHeader().getBatchId());
    summaryData.put("fileName", summaryPayload.getHeader().getJobName());
    summaryData.put("header", summaryPayload.getHeader());
    summaryData.put("processedFiles", summaryPayload.getPayload().getProcessedFiles());
    summaryData.put("printFiles", summaryPayload.getPayload().getPrintFiles());

    // If file already exists, append to an array; otherwise start a new array
    List<Map<String, Object>> summaries;
    if (jsonFile.exists()) {
        try {
            summaries = objectMapper.readValue(jsonFile, List.class);
        } catch (Exception e) {
            summaries = new ArrayList<>();
        }
    } else {
        summaries = new ArrayList<>();
    }

    summaries.add(summaryData);
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, summaries);

    summaryPayload.setSummaryFileURL(jsonFile.getAbsolutePath());
    summaryPayload.getMetadata().setSummaryFileURL(jsonFile.getAbsolutePath());

    return jsonFile;
}
