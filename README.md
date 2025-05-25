private Map<String, Object> buildFinalResponse(SummaryPayload summaryPayload) {
    Map<String, Object> finalResponse = new HashMap<>();
    finalResponse.put("message", "Batch processed successfully");
    finalResponse.put("status", "success");

    Map<String, Object> summaryPayloadMap = new HashMap<>();

    // Set batchID from header or null
    summaryPayloadMap.put("batchID", summaryPayload.getBatchId());

    // Header
    Map<String, Object> headerMap = new HashMap<>();
    HeaderInfo header = summaryPayload.getHeader();
    if (header != null) {
        headerMap.put("tenantCode", header.getTenantCode());
        headerMap.put("channelID", header.getChannelID());
        headerMap.put("audienceID", header.getAudienceID());
        headerMap.put("timestamp", header.getTimestamp());
        headerMap.put("sourceSystem", header.getSourceSystem());
        headerMap.put("product", header.getProduct());
        headerMap.put("jobName", header.getJobName());
    }
    summaryPayloadMap.put("header", headerMap);

    // Metadata
    Map<String, Object> metadataMap = new HashMap<>();
    MetaDataInfo metaData = summaryPayload.getMetaData();
    if (metaData != null) {
        metadataMap.put("totalFilesProcessed", metaData.getTotalFilesProcessed());
        metadataMap.put("processingStatus", metaData.getProcessingStatus());
        metadataMap.put("eventOutcomeCode", metaData.getEventOutcomeCode());
        metadataMap.put("eventOutcomeDescription", metaData.getEventOutcomeDescription());
    }
    summaryPayloadMap.put("metadata", metadataMap);

    // Payload
    Map<String, Object> payloadMap = new HashMap<>();
    PayloadInfo payload = summaryPayload.getPayload();
    if (payload != null) {
        payloadMap.put("uniqueConsumerRef", payload.getUniqueConsumerRef());
        payloadMap.put("uniqueECPBatchRef", payload.getUniqueECPBatchRef());
        payloadMap.put("filenetObjectID", payload.getFilenetObjectID());
        payloadMap.put("repositoryID", payload.getRepositoryID());
        payloadMap.put("runPriority", payload.getRunPriority());
        payloadMap.put("eventID", payload.getEventID());
        payloadMap.put("eventType", payload.getEventType());
        payloadMap.put("restartKey", payload.getRestartKey());
        payloadMap.put("blobURL", payload.getBlobURL());
        payloadMap.put("eventOutcomeCode", payload.getEventOutcomeCode());
        payloadMap.put("eventOutcomeDescription", payload.getEventOutcomeDescription());
    }
    summaryPayloadMap.put("payload", payloadMap);

    // Processed files
    summaryPayloadMap.put("processedFiles", summaryPayload.getCustomerSummary());

    // Summary file URL (adjust based on your actual path logic)
    summaryPayloadMap.put("summaryFileURL", "C:\\Users\\CC437236\\summary.json");

    // Timestamp (current or from summaryPayload)
    summaryPayloadMap.put("timestamp", summaryPayload.getTimestamp() != null ? summaryPayload.getTimestamp() : null);

    finalResponse.put("summaryPayload", summaryPayloadMap);
    return finalResponse;
}
