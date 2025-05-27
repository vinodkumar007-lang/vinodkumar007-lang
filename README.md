 private ApiResponse processSingleMessage(KafkaMessage message) {
        try {
            // Extract metadata and file information from KafkaMessage
            String sourceSystem = message.getSourceSystem();
            String consumerReference = message.getConsumerReference();
            String processReference = message.getProcessReference();
            String timestamp = message.getTimestamp();

            List<String> processedBlobUrls = message.getBlobURL();
            List<String> processedStatusCodes = message.getEventOutcomeCode();
            List<String> processedDescriptions = message.getEventOutcomeDescription();
            List<String> printBlobUrls = message.getPrintBlobURL();

            List<String> newProcessedUrls = new ArrayList<>();
            List<String> newPrintUrls = new ArrayList<>();

            // Upload processed files to blob storage and collect new URLs
            for (int i = 0; i < processedBlobUrls.size(); i++) {
                String newUrl = blobStorageService.copyFileFromUrlToBlob(
                        processedBlobUrls.get(i), sourceSystem, consumerReference, processReference, timestamp
                );
                newProcessedUrls.add(newUrl);
            }

            // Upload print files to blob storage
            for (String url : printBlobUrls) {
                String newUrl = blobStorageService.copyFileFromUrlToBlob(url, sourceSystem, consumerReference, processReference, timestamp);
                newPrintUrls.add(newUrl);
            }

            // Write and upload summary.json
            SummaryPayload summaryPayload = summaryJsonWriter.appendToSummaryJson(
                    message, newProcessedUrls, processedStatusCodes, processedDescriptions, newPrintUrls
            );
            String summaryFileURL = blobStorageService.uploadSummaryJson(summaryPayload, sourceSystem, consumerReference, processReference, timestamp);

            summaryPayload.setSummaryFileURL(summaryFileURL);
            return new ApiResponse("Message processed successfully", "Success", summaryPayload);

        } catch (Exception e) {
            logger.error("Error processing single Kafka message", e);
            return new ApiResponse("Failed to process message: " + e.getMessage(), "Error", null);
        }
    }
}
✅ Updated ApiResponse Class
Ensure this class uses a List<SummaryPayload>:

java
Copy
Edit
package com.nedbank.kafkaconsumer.model;

import java.util.List;

public class ApiResponse {
    private String message;
    private String status;
    private List<SummaryPayload> summaryPayload;

    public ApiResponse(String message, String status, List<SummaryPayload> summaryPayload) {
        this.message = message;
        this.status = status;
        this.summaryPayload = summaryPayload;
    }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public List<SummaryPayload> getSummaryPayload() { return summaryPayload; }
    public void setSummaryPayload(List<SummaryPayload> summaryPayload) { this.summaryPayload = summaryPayload; }
}
