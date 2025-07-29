private OTResponse callOrchestrationBatchApi(String token, String url, KafkaMessage msg) {
    OTResponse otResponse = new OTResponse();
    try {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + token);
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
        ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);

        List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get("data");
        if (data != null && !data.isEmpty()) {
            Map<String, Object> item = data.get(0);
            otResponse.setJobId((String) item.get("jobId"));
            otResponse.setId((String) item.get("id"));
            msg.setJobName(otResponse.getJobId());
            otResponse.setSuccess(true);
            return otResponse;
        } else {
            logger.error("❌ No data in OT orchestration response");
            otResponse.setSuccess(false);
            otResponse.setMessage("No data in OT orchestration response");
            return otResponse;
        }
    } catch (Exception e) {
        logger.error("❌ Failed OT Orchestration call", e);
        otResponse.setSuccess(false);
        otResponse.setMessage("Failed OT orchestration call: " + e.getMessage());
        return otResponse;
    }
}

public class OTResponse {
    private String jobId;
    private String id;
    private boolean success;
    private String message;
}

Fixed. Now returning a proper OTResponse with failure message instead of null. This will be available in the next build
