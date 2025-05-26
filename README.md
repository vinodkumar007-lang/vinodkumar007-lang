private Map<String, Object> buildBatchFinalResponse(List<SummaryPayload> payloads) {
    List<Map<String, Object>> processedList = new ArrayList<>();

    for (SummaryPayload payload : payloads) {
        processedList.add(buildFinalResponse(payload));
    }

    Map<String, Object> batchResponse = new HashMap<>();
    batchResponse.put("statusCode", "200");
    batchResponse.put("message", "Batch processed successfully");
    batchResponse.put("messagesProcessed", processedList.size());
    batchResponse.put("payloads", processedList);

    return batchResponse;
}
