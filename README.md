package com.yourpackage.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SummaryPayloadResponse {
    private String message;
    private String status;
    private SummaryPayload summaryPayload;

    public SummaryPayloadResponse() {}

    public SummaryPayloadResponse(String message, String status, SummaryPayload summaryPayload) {
        this.message = message;
        this.status = status;
        this.summaryPayload = summaryPayload;
    }

    public static SummaryPayloadResponse buildSuccess(SummaryPayload payload) {
        return new SummaryPayloadResponse("Processed successfully", "SUCCESS", payload);
    }

    public static SummaryPayloadResponse buildFailure(String errorMessage) {
        return new SummaryPayloadResponse(errorMessage, "FAILED", null);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public SummaryPayload getSummaryPayload() {
        return summaryPayload;
    }

    public void setSummaryPayload(SummaryPayload summaryPayload) {
        this.summaryPayload = summaryPayload;
    }
}
