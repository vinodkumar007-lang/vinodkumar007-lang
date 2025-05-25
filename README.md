private Map<String, Object> buildFinalResponse(SummaryPayload finalSummary) {
    Map<String, Object> responseMap = new LinkedHashMap<>();

    // Put fields at top level as requested (you can adjust field names casing if needed)
    responseMap.put("BatchID", finalSummary.getHeader().getBatchId());
    responseMap.put("TenantCode", finalSummary.getHeader().getTenantCode());
    responseMap.put("ChannelID", finalSummary.getHeader().getChannelID());
    responseMap.put("AudienceID", finalSummary.getHeader().getAudienceID());
    responseMap.put("Timestamp", new Date().toString());
    responseMap.put("SourceSystem", 
        finalSummary.getHeader().getSourceSystem() != null ? finalSummary.getHeader().getSourceSystem() : "DEBTMAN");
    responseMap.put("Product", null);  // Adjust as needed, no Product field in current model
    responseMap.put("JobName", finalSummary.getHeader().getJobName());

    responseMap.put("UniqueConsumerRef", finalSummary.getPayload().getUniqueConsumerRef());
    responseMap.put("BlobURL", null);  // No direct BlobURL field available; add if you have
    responseMap.put("FilenameOnBlobStorage", summaryFile.getName()); // Using summary file name as example
    responseMap.put("RunPriority", finalSummary.getPayload().getRunPriority());
    responseMap.put("EventType", finalSummary.getPayload().getEventType());
    responseMap.put("RestartKey", finalSummary.getPayload().getRestartKey());

    // Calculated fields
    int totalFilesProcessed = 0;
    List<CustomerSummary> customers = finalSummary.getMetaData() != null 
            ? finalSummary.getMetaData().getCustomerSummaries() : Collections.emptyList();
    for (CustomerSummary c : customers) {
        if (c.getFiles() != null) {
            totalFilesProcessed += c.getFiles().size();
        }
    }
    responseMap.put("TotalFilesProcessed", totalFilesProcessed);

    responseMap.put("ProcessingStatus", finalSummary.getHeader().getBatchStatus());

    // No source for these in current code, so left null placeholders
    responseMap.put("EventOutcomeCode", null);
    responseMap.put("EventOutcomeDescription", null);

    // Summary report file location as absolute path of summary file
    responseMap.put("SummaryReportFileLocation", summaryFile.getAbsolutePath());

    return responseMap;
}
