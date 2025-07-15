@Value("${ot.service.mfc.url}")
private String orchestrationMfcUrl;

String url;
switch (message.getSourceSystem().toUpperCase()) {
    case "DEBTMAN" -> url = orchestrationDebtmanUrl;
    case "MFC" -> url = orchestrationMfcUrl;
    default -> throw new IllegalArgumentException("Unsupported source system: " + message.getSourceSystem());
}

OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);

private OTResponse callOrchestrationBatchApi(String token, String url, KafkaMessage msg) {
    try {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + token);
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);

        ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);

        List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get("data");
        if (data != null && !data.isEmpty()) {
            Map<String, Object> item = data.get(0);
            OTResponse otResponse = new OTResponse();
            otResponse.setJobId((String) item.get("jobId"));
            otResponse.setId((String) item.get("id"));
            return otResponse;
        } else {
            logger.error("❌ No data in OT orchestration response");
        }
    } catch (Exception e) {
        logger.error("❌ Failed OT Orchestration call", e);
    }
    return null;
}
