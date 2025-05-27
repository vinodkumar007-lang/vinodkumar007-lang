private Map<String, Object> buildFinalResponse(SummaryPayload summaryPayload) {
    Map<String, Object> finalResponse = new HashMap<>();
    finalResponse.put("message", "Batch processed successfully");
    finalResponse.put("status", "success");

    Map<String, Object> summaryPayloadMap = new HashMap<>();

    // batchID - fallback empty string if null
    summaryPayloadMap.put("batchID", summaryPayload.getBatchId() != null ? summaryPayload.getBatchId() : "");

    // Header
    Map<String, Object> headerMap = new HashMap<>();
    HeaderInfo header = summaryPayload.getHeader();
    if (header != null) {
        headerMap.put("tenantCode", header.getTenantCode() != null ? header.getTenantCode() : "");
        headerMap.put("channelID", header.getChannelID() != null ? header.getChannelID() : "");
        headerMap.put("audienceID", header.getAudienceID() != null ? header.getAudienceID() : "");
        headerMap.put("timestamp", header.getTimestamp() != null ? header.getTimestamp() : "");
        headerMap.put("sourceSystem", header.getSourceSystem() != null ? header.getSourceSystem() : "");
        headerMap.put("product", header.getProduct() != null ? header.getProduct() : "");
        headerMap.put("jobName", header.getJobName() != null ? header.getJobName() : "");
    } else {
        headerMap.put("tenantCode", "");
        headerMap.put("channelID", "");
        headerMap.put("audienceID", "");
        headerMap.put("timestamp", "");
        headerMap.put("sourceSystem", "");
        headerMap.put("product", "");
        headerMap.put("jobName", "");
    }
    summaryPayloadMap.put("header", headerMap);

    // Metadata
    Map<String, Object> metadataMap = new HashMap<>();
    MetaDataInfo metaData = summaryPayload.getMetaData();
    if (metaData != null) {
        metadataMap.put("totalFilesProcessed", Math.max(metaData.getTotalFilesProcessed(), 0));
        metadataMap.put("processingStatus", metaData.getProcessingStatus() != null ? metaData.getProcessingStatus() : "");
        metadataMap.put("eventOutcomeCode", metaData.getEventOutcomeCode() != null ? metaData.getEventOutcomeCode() : "");
        metadataMap.put("eventOutcomeDescription", metaData.getEventOutcomeDescription() != null ? metaData.getEventOutcomeDescription() : "");
    } else {
        metadataMap.put("totalFilesProcessed", 0);
        metadataMap.put("processingStatus", "");
        metadataMap.put("eventOutcomeCode", "");
        metadataMap.put("eventOutcomeDescription", "");
    }
    summaryPayloadMap.put("metadata", metadataMap);

    // Payload (exclude printFiles here)
    Map<String, Object> payloadMap = new HashMap<>();
    PayloadInfo payload = summaryPayload.getPayload();
    if (payload != null) {
        payloadMap.put("uniqueConsumerRef", payload.getUniqueConsumerRef() != null ? payload.getUniqueConsumerRef() : "");
        payloadMap.put("uniqueECPBatchRef", payload.getUniqueECPBatchRef() != null ? payload.getUniqueECPBatchRef() : "");
        payloadMap.put("runPriority", payload.getRunPriority() != null ? payload.getRunPriority() : "");
        payloadMap.put("eventID", payload.getEventID() != null ? payload.getEventID() : "");
        payloadMap.put("eventType", payload.getEventType() != null ? payload.getEventType() : "");
        payloadMap.put("restartKey", payload.getRestartKey() != null ? payload.getRestartKey() : "");
        payloadMap.put("blobURL", payload.getBlobURL() != null ? payload.getBlobURL() : "");
        payloadMap.put("fileLocation", fileLocation != null ? fileLocation : "");
        payloadMap.put("eventOutcomeCode", payload.getEventOutcomeCode() != null ? payload.getEventOutcomeCode() : "");
        payloadMap.put("eventOutcomeDescription", payload.getEventOutcomeDescription() != null ? payload.getEventOutcomeDescription() : "");
        // OMIT printFiles from final response
    } else {
        payloadMap.put("uniqueConsumerRef", "");
        payloadMap.put("uniqueECPBatchRef", "");
        payloadMap.put("runPriority", "");
        payloadMap.put("eventID", "");
        payloadMap.put("eventType", "");
        payloadMap.put("restartKey", "");
        payloadMap.put("blobURL", "");
        payloadMap.put("fileLocation", "");
        payloadMap.put("eventOutcomeCode", "");
        payloadMap.put("eventOutcomeDescription", "");
    }
    summaryPayloadMap.put("payload", payloadMap);

    // DO NOT include customerSummary in final response!
    // So remove this whole block

    // summaryFileURL and timestamp
    summaryPayloadMap.put("summaryFileURL", summaryFile.getAbsolutePath());

    String ts = summaryPayload.getTimeStamp();
    if (ts == null || ts.isBlank()) {
        ts = Instant.now().toString();
    }
    summaryPayloadMap.put("timestamp", ts);

    finalResponse.put("summaryPayload", summaryPayloadMap);

    kafkaTemplate.send(outputTopic, finalResponse.toString());
    logger.info("Final Response sent to topic: {}", outputTopic);

    return finalResponse;
}
