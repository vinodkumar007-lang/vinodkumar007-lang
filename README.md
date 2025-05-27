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

    @JsonProperty("ValidationStatus")
    private String validationStatus;
}
