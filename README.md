package com.nedbank.kafka.filemanage.model;

public class SummaryPayload {
    private HeaderInfo header;
    private PayloadInfo payload;
    private MetaDataInfo metadata;

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

    public MetaDataInfo getMetadata() {
        return metadata;
    }

    public void setMetadata(MetaDataInfo metadata) {
        this.metadata = metadata;
    }
}
package com.nedbank.kafka.filemanage.model;

import java.util.List;

public class MetaDataInfo {
    private List<CustomerSummary> customerSummaries;

    public List<CustomerSummary> getCustomerSummaries() {
        return customerSummaries;
    }

    public void setCustomerSummaries(List<CustomerSummary> customerSummaries) {
        this.customerSummaries = customerSummaries;
    }
}
package com.nedbank.kafka.filemanage.model;

import java.util.List;

public class PayloadInfo {
    private List<Object> printFiles; // Typically List<String>

    public List<Object> getPrintFiles() {
        return printFiles;
    }

    public void setPrintFiles(List<Object> printFiles) {
        this.printFiles = printFiles;
    }
}
package com.nedbank.kafka.filemanage.model;

import java.util.List;

public class CustomerSummary {
    private String customerId;
    private String accountNumber;
    private List<FileDetail> files;

    public static class FileDetail {
        private String objectId;
        private String fileLocation;
        private String fileUrl;
        private boolean encrypted;
        private String status;
        private String type;

        // Getters and Setters
        public String getObjectId() { return objectId; }
        public void setObjectId(String objectId) { this.objectId = objectId; }

        public String getFileLocation() { return fileLocation; }
        public void setFileLocation(String fileLocation) { this.fileLocation = fileLocation; }

        public String getFileUrl() { return fileUrl; }
        public void setFileUrl(String fileUrl) { this.fileUrl = fileUrl; }

        public boolean isEncrypted() { return encrypted; }
        public void setEncrypted(boolean encrypted) { this.encrypted = encrypted; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
    }

    // Getters and Setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getAccountNumber() { return accountNumber; }
    public void setAccountNumber(String accountNumber) { this.accountNumber = accountNumber; }

    public List<FileDetail> getFiles() { return files; }
    public void setFiles(List<FileDetail> files) { this.files = files; }
}
package com.nedbank.kafka.filemanage.model;

public class HeaderInfo {
    private String batchId;
    private String tenantCode;
    private String channelID;
    private String audienceID;
    private String sourceSystem;
    private String product;
    private String jobName;
    private String timestamp;

    // Getters and Setters
    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }

    public String getTenantCode() { return tenantCode; }
    public void setTenantCode(String tenantCode) { this.tenantCode = tenantCode; }

    public String getChannelID() { return channelID; }
    public void setChannelID(String channelID) { this.channelID = channelID; }

    public String getAudienceID() { return audienceID; }
    public void setAudienceID(String audienceID) { this.audienceID = audienceID; }

    public String getSourceSystem() { return sourceSystem; }
    public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
}
