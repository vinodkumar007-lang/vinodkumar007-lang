private OTResponse callOrchestrationBatchApi(String token, String url, KafkaMessage msg) {
    OTResponse otResponse = new OTResponse();
    try {
        logger.info("📡 Initiating OT orchestration call to URL: {} for batchId: {} and sourceSystem: {}", 
            url, msg.getBatchId(), msg.getSourceSystem());

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + token);
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
        logger.debug("📨 OT Request Payload: {}", objectMapper.writeValueAsString(msg));

        ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);
        logger.info("✅ Received OT response with status: {} for batchId: {}", 
            response.getStatusCode(), msg.getBatchId());

        List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get("data");
        if (data != null && !data.isEmpty()) {
            Map<String, Object> item = data.get(0);
            otResponse.setJobId((String) item.get("jobId"));
            otResponse.setId((String) item.get("id"));
            msg.setJobName(otResponse.getJobId());
            otResponse.setSuccess(true);

            logger.info("🎯 OT Job created successfully - JobID: {}, ID: {}, BatchID: {}",
                otResponse.getJobId(), otResponse.getId(), msg.getBatchId());
        } else {
            logger.error("❌ No data found in OT orchestration response for batchId: {}", msg.getBatchId());
            otResponse.setSuccess(false);
            otResponse.setMessage("No data in OT orchestration response");
        }

        return otResponse;
    } catch (Exception e) {
        logger.error("❌ Exception during OT orchestration call for batchId: {} - {}", 
            msg.getBatchId(), e.getMessage(), e);
        otResponse.setSuccess(false);
        otResponse.setMessage("Failed OT orchestration call: " + e.getMessage());
        return otResponse;
    }
}
