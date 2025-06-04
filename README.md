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

import lombok.Data;

@Data
public class CustomerData {
    private String customerId;
    private String name;
    private String email;
    private String deliveryChannel;
    private String accountNumber;
    private String mobileNumber;
    private String printIndicator;
    private String tags;
    private String field;
    // add other fields as needed

    // Constructors
    public CustomerData() {}

    public CustomerData(String customerId, String name, String email, String deliveryChannel) {
        this.customerId = customerId;
        this.name = name;
        this.email = email;
        this.deliveryChannel = deliveryChannel;
    }

    public void setTenantCode(String field) {
        this.field = field;
    }
}

