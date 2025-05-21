private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl, JsonNode batchFilesNode) {
    List<ProcessedFileInfo> processedFiles = new ArrayList<>();
    List<Map<String, Object>> customerSummaries = new ArrayList<>();

    Set<String> archivedCustomers = new HashSet<>();
    Set<String> emailedCustomers = new HashSet<>();
    Set<String> mobstatCustomers = new HashSet<>();
    Set<String> printCustomers = new HashSet<>();

    for (JsonNode fileNode : batchFilesNode) {
        String objectId = fileNode.get("ObjectId").asText();
        String fileLocation = fileNode.get("fileLocation").asText();
        String extension = getFileExtension(fileLocation).toLowerCase();

        // Assuming account number is embedded in ObjectId like cust1_xxx or in metadata
        String customerId = objectId.split("_")[0];

        String dynamicFileUrl = blobUrl + "/" + objectId.replaceAll("[{}]", "") + "_" + batchId + "_" + objectId + extension;

        Map<String, Object> fileDetails = new LinkedHashMap<>();
        fileDetails.put("objectId", objectId);
        fileDetails.put("fileUrl", dynamicFileUrl);
        fileDetails.put("status", "OK");
        fileDetails.put("fileLocation", fileLocation);
        fileDetails.put("encrypted", isEncrypted(fileLocation, extension));

        switch (extension) {
            case ".pdf":
                if (fileLocation.contains("mobstat")) {
                    mobstatCustomers.add(customerId);
                    fileDetails.put("type", "pdf_mobstat");
                } else if (fileLocation.contains("archive")) {
                    archivedCustomers.add(customerId);
                    fileDetails.put("type", "pdf_archive");
                } else if (fileLocation.contains("email")) {
                    emailedCustomers.add(customerId);
                    fileDetails.put("type", "pdf_email");
                }
                break;
            case ".html":
                emailedCustomers.add(customerId);
                fileDetails.put("type", "html_email");
                fileDetails.put("status", "error?");
                break;
            case ".txt":
                emailedCustomers.add(customerId);
                fileDetails.put("type", "txt_email");
                break;
            case ".ps":
                printCustomers.add(customerId);
                fileDetails.put("type", "ps_print");
                fileDetails.put("status", "failed");
                break;
        }

        processedFiles.add(new ProcessedFileInfo(objectId, dynamicFileUrl));

        // Grouping per customer summary
        Map<String, Object> customerData = customerSummaries.stream()
                .filter(m -> customerId.equals(m.get("customerId")))
                .findFirst()
                .orElseGet(() -> {
                    Map<String, Object> newCustomerMap = new LinkedHashMap<>();
                    newCustomerMap.put("customerId", customerId);
                    newCustomerMap.put("accountNumber", ""); // Add logic if available
                    newCustomerMap.put("files", new ArrayList<Map<String, Object>>());
                    customerSummaries.add(newCustomerMap);
                    return newCustomerMap;
                });

        List<Map<String, Object>> fileList = (List<Map<String, Object>>) customerData.get("files");
        fileList.add(fileDetails);
    }

    // Constructing totals
    Map<String, Object> totals = new LinkedHashMap<>();
    totals.put("totalCustomersProcessed", customerSummaries.size());
    totals.put("totalArchived", archivedCustomers.size());
    totals.put("totalEmailed", emailedCustomers.size());
    totals.put("totalMobstat", mobstatCustomers.size());
    totals.put("totalPrint", printCustomers.size());

    SummaryPayload summary = new SummaryPayload();
    summary.setBatchID(batchId);
    summary.setHeader(new HeaderInfo());
    summary.setMetadata(new MetadataInfo());
    summary.setPayload(new PayloadInfo());
    summary.setProcessedFiles(processedFiles);
    summary.setSummaryFileURL(blobUrl + "/summary/" + batchId + "_summary.json");

    // Enrich the final payload with new per-customer and totals
    Map<String, Object> finalMap = objectMapper.convertValue(summary, Map.class);
    finalMap.put("customerSummaries", customerSummaries);
    finalMap.put("totals", totals);

    return finalMap;
}
private boolean isEncrypted(String fileLocation, String extension) {
    return (extension.equals(".pdf") || extension.equals(".html") || extension.equals(".txt")) &&
           (fileLocation.toLowerCase().contains("mobstat") || fileLocation.toLowerCase().contains("email"));
}
@JsonInclude(Include.NON_NULL)
