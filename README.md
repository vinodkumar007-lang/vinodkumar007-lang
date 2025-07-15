package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SummaryProcessedFile {
    private String customerId;            // changed from customerID
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
    private String pdfArchiveFileUrl;    // changed URL casing for consistency
    private String pdfEmailFileUrl;
    private String htmlEmailFileUrl;
    private String txtEmailFileUrl;
    private String pdfMobstatFileUrl;
    private String statusCode;
    private String statusDescription;
    private String fullName;
    private String blobURL;
}
