package com.nedbank.kafka.filemanage.model;

import lombok.Data;
import java.util.List;

@Data
public class SummaryPayload {
    private String batchID;
    private HeaderInfo header;
    private MetadataInfo metadata;
    private PayloadInfo payload;
    private List<ProcessedFileInfo> processedFiles;
    private String summaryFileURL;
    private String timestamp;
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class HeaderInfo {
    private String tenantCode;
    private String channelID;
    private String audienceID;
    private String timestamp;
    private String sourceSystem;
    private String product;
    private String jobName;
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class MetadataInfo {
    private int totalFilesProcessed;
    private String processingStatus;
    private String eventOutcomeCode;
    private String eventOutcomeDescription;
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;
import java.util.List;

@Data
public class PayloadInfo {
    private String uniqueConsumerRef;
    private String uniqueECPBatchRef;
    private List<String> filenetObjectID;
    private String repositoryID;
    private String runPriority;
    private String eventID;
    private String eventType;
    private String restartKey;
}
package com.nedbank.kafka.filemanage.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedFileInfo {
    private String customerID;
    private String pdfFileURL;
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
