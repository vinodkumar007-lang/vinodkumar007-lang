 if (url == null || url.isBlank()) {
                logger.error("❌ Orchestration URL not configured for source system '{}' in batch {}", sourceSystem, batchId);
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"URL not configured\"}");
                ack.acknowledge();
                return;
            }

            logger.info("🚀 Calling Orchestration API for batch {}: {}", batchId, url);
            OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);

            if (otResponse == null) {
                logger.error("❌ OT orchestration failed for batch {}", batchId);
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"OT call failed\"}");
                ack.acknowledge();
                return;
            }
