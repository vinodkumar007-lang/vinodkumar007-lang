private OTResponse callOrchestrationBatchApi(String token, String url, KafkaMessage msg) {
        OTResponse otResponse = new OTResponse();
        try {
            logger.info("📡 Initiating OT orchestration call to URL: {} for batchId: {} and sourceSystem: {}",
                    url, msg.getBatchId(), msg.getSourceSystem());

            HttpHeaders headers = new HttpHeaders();
            headers.set(AppConstants.HEADER_AUTHORIZATION, AppConstants.BEARER_PREFIX + token);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            logger.debug("📨 OT Request Payload: {}", objectMapper.writeValueAsString(msg));

            ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);
            logger.info("✅ Received OT response with status: {} for batchId: {}",
                    response.getStatusCode(), msg.getBatchId());

            List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get(AppConstants.OT_RESPONSE_DATA_KEY);
            if (data != null && !data.isEmpty()) {
                Map<String, Object> item = data.get(0);
                otResponse.setJobId((String) item.get(AppConstants.OT_JOB_ID_KEY));
                otResponse.setId((String) item.get(AppConstants.OT_ID_KEY));
                msg.setJobName(otResponse.getJobId());
                otResponse.setSuccess(true);

                logger.info("🎯 OT Job created successfully - JobID: {}, ID: {}, BatchID: {}",
                        otResponse.getJobId(), otResponse.getId(), msg.getBatchId());
            } else {
                logger.error("❌ No data found in OT orchestration response for batchId: {}", msg.getBatchId());
                otResponse.setSuccess(false);
                otResponse.setMessage(AppConstants.NO_OT_DATA_MESSAGE);
            }

            return otResponse;
        } catch (Exception e) {
            logger.error("❌ Exception during OT orchestration call for batchId: {} - {}",
                    msg.getBatchId(), e.getMessage(), e);
            otResponse.setSuccess(false);
            otResponse.setMessage(AppConstants.OT_CALL_FAILURE_PREFIX + e.getMessage());
            return otResponse;
        }
    }

{
    "status": "success",
    "data": {
        "id": "088612f7-db40-42ab-90f0-f50dcc1e0204",
        "domainId": "ete-SA",
        "channel": "ECPDebtmanService",
        "mode": "BATCH",
        "status": "complete",
        "flowModelType": "COMMUNICATION",
        "msg": "Batch Job",
        "externalId": null,
        "triggeredBy": "tenantadmin@exstream.role",
        "aggregatedFMCSnapshotId": "90097ec8-6a86-4812-b08b-a4b6fb594088",
        "startDate": 1757584405639,
        "endDate": 1757584411005,
        "expirationDate": null,
        "traceId": "251b4938c8590f32",
        "simulation": false
    }
}

http://exstream-deployment-orchestration-service.dev-exstream:8300/orchestration/api/v1/runtime/dev-SA/jobs/%7B%7BjobId%7D%7D
