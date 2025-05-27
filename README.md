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

    @JsonProperty("Timestamp")
    private Double timestamp;

    @JsonProperty("RunPriority")
    private String runPriority;

    @JsonProperty("EventType")
    private String eventType;

    @JsonProperty("BatchFiles")
    private List<BatchFile> batchFiles;
}

package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class BatchFile {
    private String ObjectId;
    private String RepositoryId;
    private String BlobUrl;
    private String Filename;
    private String ValidationStatus;
}
