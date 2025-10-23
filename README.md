package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMessage {

    @JsonProperty("dataStreamName")
    private String dataStreamName;

    @JsonProperty("dataStreamType")
    private String dataStreamType;

    @JsonProperty("serviceName")
    private String serviceName;

    @JsonProperty("sourceSystem")
    private String sourceSystem;

    @JsonProperty("serviceEnv")
    private String serviceEnv;

    @JsonProperty("batchId")
    private String batchId;

    @JsonProperty("tenantCode")
    private String tenantCode;

    @JsonProperty("channelID")
    private String channelID;

    @JsonProperty("audienceID")
    private String audienceID;

    @JsonProperty("product")
    private String product;

    @JsonProperty("jobName")
    private String jobName;

    @JsonProperty("consumerRef")
    private String consumerRef;

    @JsonProperty("timestamp")
    private Double timestamp;

    @JsonProperty("startTime")
    private Long startTime;

    @JsonProperty("endTime")
    private Long endTime;

    @JsonProperty("runPriority")
    private String runPriority;

    @JsonProperty("eventType")
    private String eventType;

    @JsonProperty("batchFiles")
    private List<BatchFile> batchFiles;
}

package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class BatchFile {

    @JsonProperty("objectId")
    private String objectId;

    @JsonProperty("repositoryId")
    private String repositoryId;

    @JsonProperty("blobUrl")
    private String blobUrl;

    @JsonProperty("fileName")
    private String fileName;

    @JsonProperty("fileType")
    private String fileType;

    @JsonProperty("validationStatus")
    private String validationStatus;

    @JsonProperty("customerCount")
    private int customerCount;
}
