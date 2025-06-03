package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class SummaryPayloadResponse {
    private String batchID;
    private String fileName;
    private Header header;
    private Metadata metadata;
    private String summaryFileURL;
    private String timestamp;
    private Payload payload;
    private String message;
    private String status;
    private SummaryPayload summaryPayload;

}

package com.nedbank.kafka.filemanage.model;

import lombok.Data;

import java.util.List;

@Data
public class SummaryPayload {
    private String batchID;
    private String fileName;
    private Header header;
    private Metadata metadata;
    private Payload payload;
    private List<SummaryProcessedFile> processedFiles;
    private List<PrintFile> printFiles;
    private String summaryFileURL;
    private String fileLocation;
    private String timestamp;

    // Getters and Setters
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class SummaryProcessedFile {
    private String customerID;
    private String accountNumber;
    private String pdfArchiveFileURL;
    private String pdfEmailFileURL;
    private String htmlEmailFileURL;
    private String txtEmailFileURL;
    private String pdfMobstatFileURL;
    private String statusCode;
    private String statusDescription;

    // Getters and Setters
}

package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class PrintFile {
    private String printFileURL;

    // Getters and Setters
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

import java.util.List;

@Data
public class Metadata {
    private int totalFilesProcessed;
    private String processingStatus;
    private String eventOutcomeCode;
    private String eventOutcomeDescription;
    private List<SummaryProcessedFile> processedFileList;
    private List<PrintFile> printFile;

    // Getters and Setters
}
