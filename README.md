package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SummaryPayloadResponse {
    private String message;
    private String status;
    private SummaryPayload summaryPayload;
}

package com.nedbank.kafka.filemanage.model;

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
    private String summaryFileURL;
    private String fileLocation;
    private String timestamp;
}

package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Metadata {
    private int totalFilesProcessed;
    private String processingStatus;
    private String eventOutcomeCode;
    private String eventOutcomeDescription;
}

package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
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
}
package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PrintFile {
    private String printFileURL;
}
