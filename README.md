package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.HashMap;
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
    private String blobURL;

    //private Map<String, String> fileUrls = new HashMap<>(); // archive, email, mobstat, print
    private String status; // SUCCESS / FAILED / null
    private String fileType; // for trigger file
    private String fileURL;  // for trigger file

    private String outputMethod; // ✅ fixed name

    private String overallStatus;

}
