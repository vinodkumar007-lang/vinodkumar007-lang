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

package com.nedbank.kafka.filemanage.model;

import lombok.Data;

import java.util.List;

@Data
public class CustomerSummary {
    private String customerId;
    private String accountNumber;
    private List<FileDetail> files;

    @Data
    public static class FileDetail {
        private String objectId;
        private String fileUrl;
        private String fileLocation;
        private boolean encrypted;
        private String status;
        private String type;
    }
}
