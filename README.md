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
    private Double timestamp;  // Or Long if epoch millis

    @JsonProperty("RunPriority")
    private String runPriority;

    @JsonProperty("EventType")
    private String eventType;

    @JsonProperty("BatchFiles")
    private List<BatchFile> batchFiles;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchFile {

        @JsonProperty("ObjectId")
        private String objectId;

        @JsonProperty("RepositoryId")
        private String repositoryId;

        @JsonProperty("BlobUrl")
        private String blobUrl;

        // Add other fields if needed
    }
}
