package com.nedbank.kafka.filemanage.model;

public class ProcessedFiles {

    private String customerID;
    private String accountNumber;

    private String pdfArchiveFileURL;
    private String pdfEmailFileURL;
    private String htmlEmailFileURL;
    private String txtEmailFileURL;
    private String pdfMobstatFileURL;

    private String statusCode;
    private String statusDescription;

    // Getters and setters

    public String getCustomerID() {
        return customerID;
    }

    public void setCustomerID(String customerID) {
        this.customerID = customerID;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String getPdfArchiveFileURL() {
        return pdfArchiveFileURL;
    }

    public void setPdfArchiveFileURL(String pdfArchiveFileURL) {
        this.pdfArchiveFileURL = pdfArchiveFileURL;
    }

    public String getPdfEmailFileURL() {
        return pdfEmailFileURL;
    }

    public void setPdfEmailFileURL(String pdfEmailFileURL) {
        this.pdfEmailFileURL = pdfEmailFileURL;
    }

    public String getHtmlEmailFileURL() {
        return htmlEmailFileURL;
    }

    public void setHtmlEmailFileURL(String htmlEmailFileURL) {
        this.htmlEmailFileURL = htmlEmailFileURL;
    }

    public String getTxtEmailFileURL() {
        return txtEmailFileURL;
    }

    public void setTxtEmailFileURL(String txtEmailFileURL) {
        this.txtEmailFileURL = txtEmailFileURL;
    }

    public String getPdfMobstatFileURL() {
        return pdfMobstatFileURL;
    }

    public void setPdfMobstatFileURL(String pdfMobstatFileURL) {
        this.pdfMobstatFileURL = pdfMobstatFileURL;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }

    public String getStatusDescription() {
        return statusDescription;
    }

    public void setStatusDescription(String statusDescription) {
        this.statusDescription = statusDescription;
    }
}
