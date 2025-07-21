package com.yourcompany.dto;

public class ErrorDetail {
    private String accountNumber;
    private String errorCode;
    private String errorMessage;

    public ErrorDetail() {
    }

    public ErrorDetail(String accountNumber, String errorCode, String errorMessage) {
        this.accountNumber = accountNumber;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return "ErrorDetail{" +
                "accountNumber='" + accountNumber + '\'' +
                ", errorCode='" + errorCode + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
