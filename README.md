package com.nedbank.kafka.filemanage.model;

import lombok.Data;

import java.util.List;

@Data
public class SummaryFileInfo {
    private String FileName;
    private String JobName;
    private String BatchId;
    private String Timestamp;
    private List<CustomerSummary> Customers;
    private String SummaryFileURL;
}
