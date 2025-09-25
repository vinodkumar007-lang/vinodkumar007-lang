package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import java.util.Map;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProcessedFileEntry {
    private String customerId;
    private String accountNumber;

    private String pdfArchiveFileUrl;
    private String pdfArchiveFileUrlStatus;

    private String pdfEmailFileUrl;
    private String pdfEmailFileUrlStatus;

    //private String printFileUrl;
    private String printFileUrlStatus;

    private String pdfMobstatFileUrl;
    private String pdfMobstatFileUrlStatus;

    private String overAllStatusCode;
    private String reason;

    private String finalStatus;

    private String type;
    private String blobUrl;
    private String outputType;
    private String status;

    private String outputMethod;       // e.g., EMAIL, MOBSTAT, PRINT

    private String outputBlobUrl;      // URL for EMAIL/MOBSTAT/PRINT file
    private String outputStatus;       // SUCCESS / FAILED / NOT-FOUND

    private String archiveBlobUrl;     // Always expected to be present if found
    private String archiveStatus;      // SUCCESS / FAILED / NOT-FOUND

    private String archiveOutputType;

    private String fileUrl;

    private String  emailBlobUrl;
    private String  emailStatus;
    private String  printBlobUrl;

    private String  printStatus;
    private String  mobstatBlobUrl;
    private String  mobstatStatus;
    private String overallStatus;      // SUCCESS / PARTIAL / FAILED

    private Map <String, FileStatus> fileDetails;

    private String requestedMethod;

    private String emailBlobUrlPdf;
    private String emailBlobUrlHtml;
    private String emailBlobUrlText;
    //private String statusDescription;
}

package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.Map;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SummaryProcessedFile {
    private String customerId;
    private String accountNumber;
    private String firstName;
    private String lastName;
    private String email;
    private String mobileNumber;
    private String addressLine1;
    private String addressLine2;
    private String addressLine3;
    private String postalCode;
    private String contactNumber;
    private String product;
    private String templateCode;
    private String templateName;
    private String balance;
    private String creditLimit;
    private String interestRate;
    private String dueAmount;
    private String arrears;
    private String dueDate;
    private String idNumber;
    private String accountReference;
    private String pdfArchiveFileUrl;
    private String pdfArchiveStatus;
    private String pdfEmailFileUrl;
    private String pdfEmailStatus;
    private String pdfMobstatFileUrl;
    private String pdfMobstatStatus;
    private String printFileUrl;
    private String printStatus;
    private String statusCode;
    private String statusDescription;
    private String fullName;
    private String status; // SUCCESS / FAILED / null
    private String fileType; // for trigger file
    //private String fileURL;  // for trigger file
    private String outputMethod;
    private String reason;
    private String linkedDeliveryType;
    private String linkedOutputMethod;
    private String archiveBlobUrl;
    private String archiveStatus;
    private String queueName;
    private String fileUrl;
    private String type;
    private String url;
    private String errorCode;
    private String errorMessage;
    private String outputType;
    private String errorReportEntry;
    private String  archiveOutputType;
    private String blobUrl;
    private String overallStatus;
    private String method;
    private String fileName;
    private Map<String, String> emailBlobUrls; // keys: pdf, html, text
    private String emailStatus;
    private String emailBlobUrlPdf;
    private String emailBlobUrlHtml;
    private String emailBlobUrlText;

}
