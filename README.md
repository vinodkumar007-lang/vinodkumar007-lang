{
  "dataStreamName" : "InboundListener",
  "dataStreamType" : "logs",
  "serviceName" : "InboundListener",
  "sourceSystem" : "NEDTRUST",
  "serviceEnv" : "DEV",
  "batchId" : "46cb192f-e3b0-4ced-87b7-722b6b20f58a",
  "tenantCode" : "ZANBL",
  "channelID" : "chnl007",
  "audienceID" : null,
  "product" : "NEDTRUST",
  "jobName" : "CADNT1",
  "consumerRef" : "19ef9d68-b119-5506-b09b-95a6c5fa4644",
  "timestamp" : 1761118076.452534900,
  "startTime" : 1761118022937,
  "endTime" : 1761118076452,
  "runPriority" : null,
  "eventType" : null,
  "batchFiles" : [ {
    "objectId" : "idd_6014069A-0000-C116-A99C-C1B5B6CAC35F",
    "repositoryId" : "BATCH",
    "blobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/CADNT1_251021.TXT",
    "fileName" : "CADNT1_251021.TXT",
    "fileType" : "DATA",
    "validationStatus" : "Valid",
    "customerCount" : 463
  } ]
}

===
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

    @JsonProperty("BatchId")
    private String batchId;

    @JsonProperty("SourceSystem")
    private String sourceSystem;

    @JsonProperty("SystemEnv")
    private String systemEnv;

    @JsonProperty("SystemName")
    private String systemName;

    @JsonProperty("TenantCode")
    private String tenantCode;

    @JsonProperty("ChannelID")
    private String channelID;

    @JsonProperty("AudienceID")
    private String audienceID;

    @JsonProperty("Product")
    private String product;

    @JsonProperty("JobName")
    private String jobName;

    @JsonProperty("UniqueConsumerRef")
    private String uniqueConsumerRef;

    @JsonProperty("UniqueECPBatchRef")
    private String uniqueECPBatchRef;

    @JsonProperty("Timestamp")
    private Long timestamp;

    @JsonProperty("StartTime")
    private Long startTime;

    @JsonProperty("EndTime")
    private Long endTime;

    @JsonProperty("RunPriority")
    private String runPriority;

    @JsonProperty("EventType")
    private String eventType;

    @JsonProperty("EventID")
    private String eventID;

    @JsonProperty("RestartKey")
    private String restartKey;

    @JsonProperty("BatchFiles")
    private List<BatchFile> batchFiles;

    @JsonProperty("BlobUrl")
    private String blobUrl;

}
=====
package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class BatchFile {

    @JsonProperty("ObjectId")
    private String objectId;

    @JsonProperty("RepositoryId")
    private String repositoryId;

    @JsonProperty("BlobUrl")
    private String blobUrl;

    @JsonProperty("Filename")
    private String filename;

    @JsonProperty("FileType")
    private String fileType;

    @JsonProperty("ValidationStatus")
    private String validationStatus;

    @JsonProperty("ValidationRequirement")
    private String validationRequirement;

    @JsonProperty("CustomerCount")
    private int customerCount;
}
