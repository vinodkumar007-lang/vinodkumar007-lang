private Map<String, Object> buildFinalResponse(SummaryPayload summaryPayload) {
        Map<String, Object> response = new HashMap<>();

        response.put("JobName", summaryPayload.getJobName());
        response.put("BatchId", summaryPayload.getBatchId());
        //response.put("CustomerSummary", summaryPayload.getCustomerSummary());
        //response.put("MetaData", summaryPayload.getMetaData());

        // Add Header info
        Map<String, Object> headerMap = new HashMap<>();
        HeaderInfo header = summaryPayload.getHeader();
        if (header != null) {
            headerMap.put("BatchId", header.getBatchId());
            headerMap.put("RunPriority", header.getRunPriority());
            headerMap.put("EventID", header.getEventID());
            headerMap.put("EventType", header.getEventType());
            headerMap.put("RestartKey", header.getRestartKey());
            headerMap.put("JobName", header.getJobName());
            headerMap.put("Product", header.getProduct());  // NEW field
        }
        response.put("Header", headerMap);

        // Add Payload info
        Map<String, Object> payloadMap = new HashMap<>();
        PayloadInfo payload = summaryPayload.getPayload();
        if (payload != null) {
            payloadMap.put("uniqueConsumerRef", payload.getUniqueConsumerRef());
            payloadMap.put("uniqueECPBatchRef", payload.getUniqueECPBatchRef());
            payloadMap.put("runPriority", payload.getRunPriority());
            payloadMap.put("eventID", payload.getEventID());
            payloadMap.put("eventType", payload.getEventType());
            payloadMap.put("restartKey", payload.getRestartKey());
            payloadMap.put("blobURL", payload.getBlobURL());                     // NEW
            payloadMap.put("eventOutcomeCode", payload.getEventOutcomeCode());   // NEW
            payloadMap.put("eventOutcomeDescription", payload.getEventOutcomeDescription()); // NEW
            payloadMap.put("printFiles", payload.getPrintFiles());
        }
        response.put("Payload", payloadMap);

        return response;
    }
