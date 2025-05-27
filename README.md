package com.nedbank.kafka.filemanage.model;

import lombok.Data;

import java.util.List;

@Data
public class ApiResponse {
    private final String noNewMessagesFound;
    private final String success;
    private final List<Object> objects;
    private String message;
    private String status;
    private SummaryPayload summaryPayload;

    // Getters and Setters
}
