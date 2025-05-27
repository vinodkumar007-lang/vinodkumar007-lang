private Map<String, Object> buildFinalResponse(SummaryPayload summaryPayload) {
    Map<String, Object> response = new LinkedHashMap<>();

    response.put("jobName", summaryPayload.getJobName());
    response.put("batchId", summaryPayload.getBatchId());

    // Header
    Map<String, Object> headerMap = new LinkedHashMap<>();
    HeaderInfo header = summaryPayload.getHeader();
    if (header != null) {
        headerMap.put("batchId", header.getBatchId());
        headerMap.put("runPriority", header.getRunPriority());
        headerMap.put("eventID", header.getEventID());
        headerMap.put("eventType", header.getEventType());
        headerMap.put("restartKey", header.getRestartKey());
        headerMap.put("jobName", header.getJobName());
        headerMap.put("product", header.getProduct());
    }
    response.put("header", headerMap);

    // Metadata
    Map<String, Object> metadataMap = new LinkedHashMap<>();
    MetaDataInfo meta = summaryPayload.getMetaData();
    if (meta != null) {
        metadataMap.put("totalCustomers", meta.getTotalCustomers());
        metadataMap.put("totalFiles", meta.getTotalFiles());
    }
    response.put("metaData", metadataMap);

    // Payload
    Map<String, Object> payloadMap = new LinkedHashMap<>();
    PayloadInfo payload = summaryPayload.getPayload();
    if (payload != null) {
        payloadMap.put("uniqueConsumerRef", payload.getUniqueConsumerRef());
        payloadMap.put("uniqueECPBatchRef", payload.getUniqueECPBatchRef());
        payloadMap.put("runPriority", payload.getRunPriority());
        payloadMap.put("eventID", payload.getEventID());
        payloadMap.put("eventType", payload.getEventType());
        payloadMap.put("restartKey", payload.getRestartKey());
        payloadMap.put("blobURL", payload.getBlobURL());
        payloadMap.put("eventOutcomeCode", payload.getEventOutcomeCode());
        payloadMap.put("eventOutcomeDescription", payload.getEventOutcomeDescription());
        payloadMap.put("printFiles", payload.getPrintFiles());
    }
    response.put("payload", payloadMap);

    // CustomerSummary
    List<Map<String, Object>> customerSummaries = new ArrayList<>();
    if (summaryPayload.getCustomerSummary() != null) {
        for (CustomerSummary customer : summaryPayload.getCustomerSummary()) {
            Map<String, Object> customerMap = new LinkedHashMap<>();
            customerMap.put("customerId", customer.getCustomerId());
            customerMap.put("accountNumber", customer.getAccountNumber());

            List<Map<String, Object>> filesList = new ArrayList<>();
            for (CustomerSummary.FileDetail fileDetail : customer.getFiles()) {
                Map<String, Object> fileMap = new LinkedHashMap<>();
                fileMap.put("objectId", fileDetail.getObjectId());
                fileMap.put("fileLocation", fileDetail.getFileLocation());
                fileMap.put("fileUrl", fileDetail.getFileUrl());
                fileMap.put("encrypted", fileDetail.isEncrypted());
                fileMap.put("status", fileDetail.getStatus());
                fileMap.put("type", fileDetail.getType());
                filesList.add(fileMap);
            }

            customerMap.put("files", filesList);
            customerSummaries.add(customerMap);
        }
    }
    response.put("customerSummary", customerSummaries);

    // Optional fields (if available from setTopic, setPartition, setOffset)
    response.put("kafkaTopic", summaryPayload.getTopic());
    response.put("partition", summaryPayload.getPartition());
    response.put("offset", summaryPayload.getOffset());

    return response;
}
