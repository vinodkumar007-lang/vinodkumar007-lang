private SummaryPayload processSingleMessage(String message) throws IOException {
    JsonNode root = objectMapper.readTree(message);

    // Read batchId and jobName from message, fallback to default
    String batchId = safeGetText(root, "BatchId", false);
    if (batchId == null) batchId = UUID.randomUUID().toString();

    String jobName = safeGetText(root, "JobName", false);
    if (jobName == null) jobName = "";

    // Extract header fields from root
    HeaderInfo headerInfo = new HeaderInfo();
    headerInfo.setBatchId(batchId);
    headerInfo.setTenantCode(safeGetText(root, "TenantCode", false));
    headerInfo.setChannelID(safeGetText(root, "ChannelID", false));
    headerInfo.setAudienceID(safeGetText(root, "AudienceID", false));
    headerInfo.setTimestamp(safeGetText(root, "Timestamp", false));
    headerInfo.setSourceSystem(safeGetText(root, "SourceSystem", false));
    headerInfo.setProduct(safeGetText(root, "Product", false));
    headerInfo.setJobName(jobName);

    // Metadata info
    MetaDataInfo metaDataInfo = new MetaDataInfo();
    metaDataInfo.setTotalFilesProcessed(safeGetInt(root, "TotalFilesProcessed", 0));
    metaDataInfo.setProcessingStatus(safeGetText(root, "ProcessingStatus", false));
    metaDataInfo.setEventOutcomeCode(safeGetText(root, "EventOutcomeCode", false));
    metaDataInfo.setEventOutcomeDescription(safeGetText(root, "EventOutcomeDescription", false));

    // Payload info
    PayloadInfo payloadInfo = new PayloadInfo();
    payloadInfo.setUniqueConsumerRef(safeGetText(root, "UniqueConsumerRef", false));
    payloadInfo.setUniqueECPBatchRef(safeGetText(root, "UniqueECPBatchRef", false));
    payloadInfo.setRunPriority(safeGetText(root, "RunPriority", false));
    payloadInfo.setEventID(safeGetText(root, "EventID", false));
    payloadInfo.setEventType(safeGetText(root, "EventType", false));
    payloadInfo.setRestartKey(safeGetText(root, "RestartKey", false));

    // Extract customer summaries and file details here (your existing logic)...

    SummaryPayload summaryPayload = new SummaryPayload();
    summaryPayload.setBatchID(batchId);
    summaryPayload.setHeader(headerInfo);
    summaryPayload.setMetadata(metaDataInfo);
    summaryPayload.setPayload(payloadInfo);
    summaryPayload.setSummaryURL(summaryFile.getAbsolutePath());
    summaryPayload.setTimestamp(String.valueOf(System.currentTimeMillis()));

    return summaryPayload;
}

private int safeGetInt(JsonNode node, String fieldName, int defaultVal) {
    if (node != null && node.has(fieldName) && node.get(fieldName).canConvertToInt()) {
        return node.get(fieldName).asInt();
    }
    return defaultVal;
}
