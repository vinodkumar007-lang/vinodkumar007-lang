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
            String runtimeUrl = String.format(
                    "http://exstream-deployment-orchestration-service.dev-exstream:8300/orchestration/api/v1/runtime/dev-SA/jobs/%s",
                    otResponse.getJobId()
            );

            boolean completed = false;
            int maxRetries = 60;              // up to 30 minutes if poll interval = 30s
            long pollIntervalMillis = 30_000; // 30 seconds
            int retryCount = 0;

            while (!completed && retryCount < maxRetries) {
                try {
                    ResponseEntity<Map> runtimeResponse = restTemplate.exchange(runtimeUrl, HttpMethod.GET,
                            new HttpEntity<>(headers), Map.class);

                    Map<String, Object> runtimeBody = runtimeResponse.getBody();
                    if (runtimeBody != null && "success".equals(runtimeBody.get("status"))) {
                        Map<String, Object> runtimeData = (Map<String, Object>) runtimeBody.get("data");
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

private File waitForXmlFile(String jobId, String id) throws InterruptedException {
    Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, AppConstants.DOCGEN_FOLDER);
    long startTime = System.currentTimeMillis();

    while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
        if (Files.exists(docgenRoot)) {
            try (Stream<Path> paths = Files.walk(docgenRoot)) {
                Optional<Path> xmlPathOpt = paths
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().equalsIgnoreCase(AppConstants.XML_FILE_NAME))
                        .findFirst();

                if (xmlPathOpt.isPresent()) {
                    File xmlFile = xmlPathOpt.get().toFile();
                    logger.info("[{}] 📄 Found generated XML file: {}", jobId, xmlFile.getAbsolutePath());
                    return xmlFile;
                }
            } catch (IOException e) {
                logger.warn(AppConstants.LOG_ERROR_SCANNING_FOLDER, jobId, id, e.getMessage(), e);
            }
        } else {
            logger.debug(AppConstants.LOG_DOCGEN_FOLDER_NOT_FOUND, jobId, id, docgenRoot);
        }

        TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
    }

    String errMsg = String.format(AppConstants.LOG_XML_TIMEOUT, docgenRoot, jobId, id);
    logger.error(errMsg);
    throw new IllegalStateException(errMsg);
}

2025-09-11T15:52:03.347+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Received OT response with status: 201 CREATED for batchId: 076f2b3c-37bc-4bcb-ab6a-29041acfc0f9
2025-09-11T15:52:03.347+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 🎯 OT Job created successfully - JobID: 8f9ee8fe-c512-44f3-9caf-786020b42e2b, ID: df625d4a-2da4-4d90-8e34-75a48f42318e, BatchID: 076f2b3c-37bc-4bcb-ab6a-29041acfc0f9
2025-09-11T15:52:03.902+02:00  WARN 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ⚠️ Runtime status check failed for JobID 8f9ee8fe-c512-44f3-9caf-786020b42e2b - class java.util.ArrayList cannot be cast to class java.util.Map (java.util.ArrayList and java.util.Map are in module java.base of loader 'bootstrap')


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
            String runtimeUrl = String.format(
                    "http://exstream-deployment-orchestration-service.dev-exstream:8300/orchestration/api/v1/runtime/dev-SA/jobs/%s",
                    otResponse.getJobId()
            );

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




