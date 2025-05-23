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

public class HeaderInfo {
    private String BatchId;
    private String TenantCode;
    private String ChannelID;
    private String AudienceID;
    private String SourceSystem;
    private String Product;
    private String JobName;
    private String Timestamp;

    public String getBatchId() {
        return BatchId;
    }

    public void setBatchId(String batchId) {
        BatchId = batchId;
    }

    public String getTenantCode() {
        return TenantCode;
    }

    public void setTenantCode(String tenantCode) {
        TenantCode = tenantCode;
    }

    public String getChannelID() {
        return ChannelID;
    }

    public void setChannelID(String channelID) {
        ChannelID = channelID;
    }

    public String getAudienceID() {
        return AudienceID;
    }

    public void setAudienceID(String audienceID) {
        AudienceID = audienceID;
    }

    public String getSourceSystem() {
        return SourceSystem;
    }

    public void setSourceSystem(String sourceSystem) {
        SourceSystem = sourceSystem;
    }

    public String getProduct() {
        return Product;
    }

    public void setProduct(String product) {
        Product = product;
    }

    public String getJobName() {
        return JobName;
    }

    public void setJobName(String jobName) {
        JobName = jobName;
    }

    public String getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(String timestamp) {
        Timestamp = timestamp;
    }
}
package com.nedbank.kafka.filemanage.model;

import java.util.List;

public class PayloadInfo {
    private List<Object> printFiles;  // Assuming List<Object>, adjust type if needed

    public List<Object> getPrintFiles() {
        return printFiles;
    }

    public void setPrintFiles(List<Object> printFiles) {
        this.printFiles = printFiles;
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

public class CustomerSummary {
    private String customerId;
    private String accountNumber;
    private List<FileDetail> files;

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public List<FileDetail> getFiles() {
        return files;
    }

    public void setFiles(List<FileDetail> files) {
        this.files = files;
    }

    public static class FileDetail {
        private String objectId;
        private String fileLocation;
        private String fileUrl;
        private boolean encrypted;
        private String status;
        private String type;
        private String repositoryId;

        public String getObjectId() {
            return objectId;
        }

        public void setObjectId(String objectId) {
            this.objectId = objectId;
        }

        public String getFileLocation() {
            return fileLocation;
        }

        public void setFileLocation(String fileLocation) {
            this.fileLocation = fileLocation;
        }

        public String getFileUrl() {
            return fileUrl;
        }

        public void setFileUrl(String fileUrl) {
            this.fileUrl = fileUrl;
        }

        public boolean isEncrypted() {
            return encrypted;
        }

        public void setEncrypted(boolean encrypted) {
            this.encrypted = encrypted;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getRepositoryId() {
            return repositoryId;
        }

        public void setRepositoryId(String repositoryId) {
            this.repositoryId = repositoryId;
        }
    }
}
