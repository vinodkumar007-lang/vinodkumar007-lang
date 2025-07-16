package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SummaryPayload {
    private String batchID;
    private String fileName;
    private Header header;
    private Metadata metadata;
    private Payload payload;
    private List<SummaryProcessedFile> processedFiles;
    private List<PrintFile> printFiles;
    private String mobstatTriggerFile;
    
    @JsonIgnore
    private String summaryFileURL; // Will not be written to summary.json

    private String fileLocation;
    private String timestamp;
}
