private Map<String, Object> buildFinalResponse(SummaryPayload summaryPayload) {
    Map<String, Object> response = new HashMap<>();
    response.put("status", "success");
    response.put("message", "Processed all unprocessed messages.");

    Map<String, Object> summaryMap = new HashMap<>();
    summaryMap.put("batchID", summaryPayload.getBatchID());
    summaryMap.put("fileName", summaryPayload.getFileName());
    summaryMap.put("timestamp", summaryPayload.getTimestamp());
    summaryMap.put("summaryFileURL", summaryPayload.getSummaryFileURL());

    Map<String, Object> headerMap = new HashMap<>();
    headerMap.put("sourceSystem", summaryPayload.getSourceSystem());
    headerMap.put("consumerReference", summaryPayload.getConsumerReference());
    headerMap.put("processReference", summaryPayload.getProcessReference());
    headerMap.put("timestamp", summaryPayload.getHeaderTimestamp());
    summaryMap.put("header", headerMap);

    Map<String, Object> payloadMap = new HashMap<>();
    payloadMap.put("processedFiles", summaryPayload.getProcessedFiles());
    // REMOVED: payloadMap.put("printFiles", summaryPayload.getPrintFiles());
    summaryMap.put("payload", payloadMap);

    response.put("summaryPayload", summaryMap);

    return response;
}
