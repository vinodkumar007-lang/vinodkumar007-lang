package com.nedbank.kafka.filemanage.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApiResponse {
    private String message;     // e.g., "Batch processed successfully"
    private String status;      // e.g., "success", "error", "info"
    private Object data;        // can be SummaryPayload, List<Something>, or null
}
