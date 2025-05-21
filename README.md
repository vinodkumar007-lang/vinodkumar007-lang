private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl, JsonNode batchFilesNode) {
    List<ProcessedFileInfo> processedFiles = new ArrayList<>();
    List<CustomerSummary> customerSummaries = new ArrayList<>();

    Set<String> archivedCustomers = new HashSet<>();
    Set<String> emailedCustomers = new HashSet<>();
    Set<String> mobstatCustomers = new HashSet<>();
    Set<String> printCustomers = new HashSet<>();

    String fileName = "";
    String jobName = "";
    String tenantCode = "ZANBL"; // Can be extracted dynamically if present
    String channelId = "100";
    String sourceSystem = "CARD";
    String product = "CASA";
    String audienceId = UUID.randomUUID().toString();
    String uniqueConsumerRef = UUID.randomUUID().toString();
    String uniqueECPBatchRef = UUID.randomUUID().toString();
    String repositoryId = "Legacy";
    String runPriority = "High";
    String eventType = "Completion";
    String eventId = "E12345";
    String restartKey = "Key_" + batchId;

    for (JsonNode fileNode : batchFilesNode) {
        String objectId = fileNode.get("ObjectId").asText();
        String fileLocation = fileNode.get("fileLocation").asText();
        String extension = getFileExtension(fileLocation).toLowerCase();
        String customerId = objectId.split("_")[0];

        if (fileNode.has("fileName")) fileName = fileNode.get("fileName").asText();
        if (fileNode.has("jobName")) jobName = fileNode.get("jobName").asText();

        String dynamicFileUrl = "file://" + fileLocation;

        CustomerSummary.FileDetail fileDetail = new CustomerSummary.FileDetail();
        fileDetail.setObjectId(objectId);
        fileDetail.setFileUrl(dynamicFileUrl);
        fileDetail.setFileLocation(fileLocation);
        fileDetail.setStatus(extension.equals(".ps") ? "failed" : "OK");
        fileDetail.setEncrypted(isEncrypted(fileLocation, extension));
        fileDetail.setType(determineType(fileLocation, extension));

        if (fileLocation.contains("mobstat")) mobstatCustomers.add(customerId);
        if (fileLocation.contains("archive")) archivedCustomers.add(customerId);
        if (fileLocation.contains("email")) emailedCustomers.add(customerId);
        if (extension.equals(".ps")) printCustomers.add(customerId);

        processedFiles.add(new ProcessedFileInfo(customerId, dynamicFileUrl));

        CustomerSummary customer = customerSummaries.stream()
                .filter(c -> c.getCustomerId().equals(customerId))
                .findFirst()
                .orElseGet(() -> {
                    CustomerSummary newCustomer = new CustomerSummary();
                    newCustomer.setCustomerId(customerId);
                    newCustomer.setAccountNumber("");
                    newCustomer.setFiles(new ArrayList<>());
                    customerSummaries.add(newCustomer);
                    return newCustomer;
                });

        customer.getFiles().add(fileDetail);
    }

    // Create full detailed summary for file output
    Map<String, Object> fullSummary = new LinkedHashMap<>();
    fullSummary.put("fileName", fileName);
    fullSummary.put("jobName", jobName);
    fullSummary.put("batchId", batchId);
    fullSummary.put("timestamp", new Date().toString());
    fullSummary.put("customers", customerSummaries);

    Map<String, Object> totals = new LinkedHashMap<>();
    totals.put("totalCustomersProcessed", customerSummaries.size());
    totals.put("totalArchived", archivedCustomers.size());
    totals.put("totalEmailed", emailedCustomers.size());
    totals.put("totalMobstat", mobstatCustomers.size());
    totals.put("totalPrint", printCustomers.size());

    fullSummary.put("totals", totals);

    // Save to file locally
    String summaryFileUrl = null;
    try {
        String userHome = System.getProperty("user.home");
        File outputDir = new File(userHome + File.separator + "summary_outputs");
        if (!outputDir.exists()) outputDir.mkdirs();

        String filePath = outputDir + File.separator + "summary_" + batchId + ".json";
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(filePath), fullSummary);
        logger.info("Summary JSON written to local file: {}", filePath);

        // Open in default viewer (Windows only)
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            Runtime.getRuntime().exec(new String[]{"cmd", "/c", "start", "", filePath});
        }

        summaryFileUrl = "file://" + filePath.replace("\\", "/");

    } catch (IOException e) {
        logger.error("Failed to write local summary JSON file", e);
    }

    // Final summary to return in API/Kafka
    SummaryPayload summary = new SummaryPayload();
    summary.setBatchID(batchId);
    summary.setHeader(new HeaderInfo(tenantCode, channelId, audienceId, new Date(), sourceSystem, product, jobName));
    summary.setMetadata(new MetadataInfo(
            customerSummaries.size(), // total files/customers processed
            "Success",
            "Success",
            "All customer PDFs processed successfully"
    ));
    summary.setPayload(new PayloadInfo(
            uniqueConsumerRef,
            uniqueECPBatchRef,
            List.of(uniqueECPBatchRef), // FilenetObjectID
            repositoryId,
            runPriority,
            eventId,
            eventType,
            restartKey
    ));
    summary.setProcessedFiles(processedFiles);
    summary.setSummaryFileURL(summaryFileUrl);
    summary.setTimestamp(new Date());

    return objectMapper.convertValue(summary, Map.class);
}
