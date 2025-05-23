package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PayloadInfo {
    @JsonProperty("uniqueConsumerRef")
    private String uniqueConsumerRef;

    @JsonProperty("uniqueECPBatchRef")
    private String uniqueECPBatchRef;

    private String runPriority;

    private String eventID;

    private String eventType;

    private String restartKey;

    @JsonProperty("printFiles")
    private List<String> printFiles;

    // Getters & Setters
    public String getUniqueConsumerRef() {
        return uniqueConsumerRef;
    }

    public void setUniqueConsumerRef(String uniqueConsumerRef) {
        this.uniqueConsumerRef = uniqueConsumerRef;
    }

    public String getUniqueECPBatchRef() {
        return uniqueECPBatchRef;
    }

    public void setUniqueECPBatchRef(String uniqueECPBatchRef) {
        this.uniqueECPBatchRef = uniqueECPBatchRef;
    }

    public String getRunPriority() {
        return runPriority;
    }

    public void setRunPriority(String runPriority) {
        this.runPriority = runPriority;
    }

    public String getEventID() {
        return eventID;
    }

    public void setEventID(String eventID) {
        this.eventID = eventID;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getRestartKey() {
        return restartKey;
    }

    public void setRestartKey(String restartKey) {
        this.restartKey = restartKey;
    }

    public List<String> getPrintFiles() {
        return printFiles;
    }

    public void setPrintFiles(List<String> printFiles) {
        this.printFiles = printFiles;
    }
}
package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class HeaderInfo {
    private String jobName;
    private String batchId;
    private String batchStatus;
    private String sourceSystem;
    private String queueName;

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
}
package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SummaryPayload {
    private HeaderInfo header;
    private PayloadInfo payload;
    private MetaDataInfo metaData;

    public HeaderInfo getHeader() {
        return header;
    }

    public void setHeader(HeaderInfo header) {
        this.header = header;
    }

    public PayloadInfo getPayload() {
        return payload;
    }

    public void setPayload(PayloadInfo payload) {
        this.payload = payload;
    }

    public MetaDataInfo getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaDataInfo metaData) {
        this.metaData = metaData;
    }
}
