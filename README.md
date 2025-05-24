package com.nedbank.kafka.filemanage.model;

public class HeaderInfo {

    private String jobName;
    private String batchId;
    private String batchStatus;
    private String sourceSystem;
    private String queueName;

    // ✅ Newly added fields
    private String tenantCode;
    private String channelID;
    private String audienceID;

    // Getters and setters
    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getBatchStatus() {
        return batchStatus;
    }

    public void setBatchStatus(String batchStatus) {
        this.batchStatus = batchStatus;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public void setSourceSystem(String sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getTenantCode() {
        return tenantCode;
    }

    public void setTenantCode(String tenantCode) {
        this.tenantCode = tenantCode;
    }

    public String getChannelID() {
        return channelID;
    }

    public void setChannelID(String channelID) {
        this.channelID = channelID;
    }

    public String getAudienceID() {
        return audienceID;
    }

    public void setAudienceID(String audienceID) {
        this.audienceID = audienceID;
    }
}
private HeaderInfo buildHeader(JsonNode node, String jobName) {
    HeaderInfo header = new HeaderInfo();
    header.setJobName(jobName != null ? jobName : safeGetText(node, "JobName", false));
    header.setBatchId(safeGetText(node, "BatchId", false));
    header.setBatchStatus(safeGetText(node, "BatchStatus", false));
    header.setSourceSystem(safeGetText(node, "SourceSystem", false));
    header.setQueueName(safeGetText(node, "QueueName", false));

    // ✅ Added fields from message
    header.setTenantCode(safeGetText(node, "tenantCode", false));
    header.setChannelID(safeGetText(node, "channelID", false));
    header.setAudienceID(safeGetText(node, "audienceID", false));

    return header;
}
private Map<String, Object> buildFinalResponse(SummaryPayload finalSummary) {
    Map<String, Object> responseMap = new LinkedHashMap<>();
    responseMap.put("message", "Batch processed successfully");
    responseMap.put("status", "success");

    Map<String, Object> summaryPayload = new LinkedHashMap<>();

    summaryPayload.put("batchID", finalSummary.getHeader().getBatchId());

    Map<String, Object> header = new LinkedHashMap<>();
    header.put("tenantCode", finalSummary.getHeader().getTenantCode());
    header.put("channelID", finalSummary.getHeader().getChannelID());
    header.put("audienceID", finalSummary.getHeader().getAudienceID());
    header.put("timestamp", new Date().toString());
    header.put("sourceSystem", finalSummary.getHeader().getSourceSystem() != null ? finalSummary.getHeader().getSourceSystem() : "DEBTMAN");
    header.put("product", null); // Adjust if needed
    header.put("jobName", finalSummary.getHeader().getJobName());

    summaryPayload.put("header", header);

    Map<String, Object> metadata = new LinkedHashMap<>();
    List<CustomerSummary> customers = finalSummary.getMetaData().getCustomerSummaries();
    metadata.put("totalFilesProcessed", customers.stream().mapToInt(cs -> cs.getFiles().size()).sum());
    metadata.put("processingStatus", finalSummary.getHeader().getBatchStatus());
    metadata.put("eventOutcomeCode", null); // Populate if available
    metadata.put("eventOutcomeDescription", null); // Populate if available

    summaryPayload.put("metadata", metadata);

    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("uniqueConsumerRef", finalSummary.getPayload().getUniqueConsumerRef());
    payload.put("uniqueECPBatchRef", finalSummary.getPayload().getUniqueECPBatchRef());
    payload.put("filenetObjectID", null);  // You mentioned this is not required to extract
    payload.put("repositoryID", null);     // You mentioned this is not required to extract
    payload.put("runPriority", finalSummary.getPayload().getRunPriority());
    payload.put("eventID", finalSummary.getPayload().getEventID());
    payload.put("eventType", finalSummary.getPayload().getEventType());
    payload.put("restartKey", finalSummary.getPayload().getRestartKey());

    summaryPayload.put("payload", payload);

    // Processed files
    List<Map<String, Object>> processedFiles = new ArrayList<>();
    for (CustomerSummary cs : customers) {
        for (CustomerSummary.FileDetail file : cs.getFiles()) {
            Map<String, Object> fileMap = new LinkedHashMap<>();
            fileMap.put("objectId", file.getObjectId());
            fileMap.put("fileLocation", file.getFileLocation());
            fileMap.put("fileUrl", file.getFileUrl());
            fileMap.put("encrypted", file.isEncrypted());
            fileMap.put("status", file.getStatus());
            fileMap.put("type", file.getType());
            processedFiles.add(fileMap);
        }
    }

    summaryPayload.put("processedFiles", processedFiles);
    summaryPayload.put("summaryFileURL", summaryFile.getAbsolutePath());
    summaryPayload.put("timestamp", new Date().toString());

    responseMap.put("summaryPayload", summaryPayload);

    return responseMap;
}
