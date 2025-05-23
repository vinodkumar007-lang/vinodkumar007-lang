private File writeSummaryToFile(SummaryPayload summaryPayload) throws IOException {
    String userHome = System.getProperty("user.home");
    File jsonFile = new File(userHome, "summary.json");

    Map<String, Object> summaryData = new HashMap<>();
    summaryData.put("batchID", summaryPayload.getHeader().getBatchId());
    summaryData.put("fileName", summaryPayload.getHeader().getJobName());
    summaryData.put("header", summaryPayload.getHeader());
    summaryData.put("processedFiles", summaryPayload.getPayload().getProcessedFiles());
    summaryData.put("printFiles", summaryPayload.getPayload().getPrintFiles());

    // Additional summary fields extracted from header and metadata
    Map<String, Object> summaryBasicInfo = new HashMap<>();
    HeaderInfo header = summaryPayload.getHeader();

    summaryBasicInfo.put("batchID", header.getBatchId());
    summaryBasicInfo.put("sourceSystem", header.getSourceSystem());
    summaryBasicInfo.put("tenantCode", header.getTenantCode());
    summaryBasicInfo.put("uniqueConsumerRef", header.getBatchId()); // or correct field
    summaryBasicInfo.put("timestamp", header.getTimestamp());

    // Count total files from all customers
    int fileCount = 0;
    if (summaryPayload.getMetadata() != null && summaryPayload.getMetadata().getCustomerSummaries() != null) {
        for (CustomerSummary cs : summaryPayload.getMetadata().getCustomerSummaries()) {
            fileCount += (cs.getFiles() != null) ? cs.getFiles().size() : 0;
        }
    }
    summaryBasicInfo.put("fileCount", fileCount);

    // Example validationStatus
    String validationStatus = "OK"; // Adjust if you have actual status info
    summaryBasicInfo.put("validationStatus", validationStatus);

    summaryData.put("summaryBasicInfo", summaryBasicInfo);

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
