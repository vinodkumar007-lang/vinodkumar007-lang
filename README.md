package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class BatchFile {
    private String ObjectId;
    private String RepositoryId;
    private String BlobUrl;
    private String Filename;
    private String ValidationStatus;

    // Getters and Setters
}
