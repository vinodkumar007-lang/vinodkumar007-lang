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

            List<Map<String, Object>> data =
                    (List<Map<String, Object>>) response.getBody().get(AppConstants.OT_RESPONSE_DATA_KEY);

            if (data != null && !data.isEmpty()) {
                Map<String, Object> item = data.get(0);
                otResponse.setJobId((String) item.get(AppConstants.OT_JOB_ID_KEY));
                otResponse.setId((String) item.get(AppConstants.OT_ID_KEY));
                msg.setJobName(otResponse.getJobId());

                logger.info("🎯 OT Job created successfully - JobID: {}, ID: {}, BatchID: {}",
                        otResponse.getJobId(), otResponse.getId(), msg.getBatchId());

                // 🔄 Poll runtime API until status=complete
                String runtimeUrl = runtimeBaseUrl + otResponse.getJobId();

                boolean completed = false;
                int maxRetries = 60;              // up to 30 minutes if poll interval = 30s
                long pollIntervalMillis = 30_000; // 30 seconds
                int retryCount = 0;

                while (!completed && retryCount < maxRetries) {
                    try {
                        ResponseEntity<Map> runtimeResponse = restTemplate.exchange(
                                runtimeUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);

                        Map<String, Object> runtimeBody = runtimeResponse.getBody();
                        if (runtimeBody != null && "success".equals(runtimeBody.get("status"))) {
                            Object dataObj = runtimeBody.get("data");

                            if (dataObj instanceof Map) {
                                Map<String, Object> runtimeData = (Map<String, Object>) dataObj;
                                String jobStatus = (String) runtimeData.get("status");

                                logger.info("🔎 Runtime check attempt {} for JobID {} => status={}",
                                        retryCount + 1, otResponse.getJobId(), jobStatus);

                                if ("complete".equalsIgnoreCase(jobStatus)) {
                                    logger.info("✅ Runtime job completed for JobID: {} (BatchID: {})",
                                            otResponse.getJobId(), msg.getBatchId());
                                    completed = true;
                                    otResponse.setSuccess(true);
                                    break;
                                }
                            } else {
                                logger.error("❌ Unexpected runtime response format: data is not a Map (JobID={})",
                                        otResponse.getJobId());
                            }
                        }
                    } catch (Exception ex) {
                        logger.warn("⚠️ Runtime status check failed for JobID {} - {}",
                                otResponse.getJobId(), ex.getMessage());
                    }

                    retryCount++;
                    Thread.sleep(pollIntervalMillis);
                }

                if (!completed) {
                    String errMsg = String.format(
                            "❌ Job did not complete within %d minutes (JobID=%s, BatchID=%s)",
                            (maxRetries * pollIntervalMillis) / 60000,
                            otResponse.getJobId(),
                            msg.getBatchId()
                    );
                    logger.error(errMsg);
                    throw new RuntimeException(errMsg); // 🚨 discard flow
                }

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
