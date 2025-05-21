package com.nedbank.kafka.filemanage.model;

import lombok.Data;
import java.util.Date;
import java.util.List;

@Data
public class SummaryPayload {
    private String BatchID;
    private HeaderInfo Header;
    private MetadataInfo Metadata;
    private PayloadInfo Payload;
    private List<ProcessedFileInfo> ProcessedFiles;
    private String SummaryFileURL;
    private Date Timestamp;
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;
import java.util.Date;

@Data
public class HeaderInfo {
    private String TenantCode;
    private String ChannelID;
    private String AudienceID;
    private Date Timestamp;
    private String SourceSystem;
    private String Product;
    private String JobName;

    public HeaderInfo(String tenantCode, String channelID, String audienceID, Date timestamp,
                      String sourceSystem, String product, String jobName) {
        this.TenantCode = tenantCode;
        this.ChannelID = channelID;
        this.AudienceID = audienceID;
        this.Timestamp = timestamp;
        this.SourceSystem = sourceSystem;
        this.Product = product;
        this.JobName = jobName;
    }
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class MetadataInfo {
    private Integer TotalFilesProcessed;
    private String ProcessingStatus;
    private String EventOutcomeCode;
    private String EventOutcomeDescription;
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class PayloadInfo {
    private String UniqueConsumerRef;
    private String UniqueECPBatchRef;
    private String FilenetObjectID;
    private String RepositoryID;
    private String RunPriority;
    private String EventID;
    private String EventType;
    private String RestartKey;
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class ProcessedFileInfo {
    private String ObjectId;
    private String FileUrl;

    public ProcessedFileInfo(String objectId, String fileUrl) {
        this.ObjectId = objectId;
        this.FileUrl = fileUrl;
    }
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;
import java.util.List;

@Data
public class CustomerSummary {
    private String CustomerId;
    private String AccountNumber;
    private List<FileDetail> Files;

    @Data
    public static class FileDetail {
        private String ObjectId;
        private String FileUrl;
        private String FileLocation;
        private String Status;
        private Boolean Encrypted;
        private String Type;
    }
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class SummaryFileInfo {
    private String FileName;
    private String JobName;
    private String BatchId;
    private String Timestamp;
    private List<CustomerSummary> Customers;
    private String SummaryFileURL;
}
