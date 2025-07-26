public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        try {
            logger.info("\uD83D\uDCE5 Received Kafka message: {}", rawMessage);
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);
            logger.info("\uD83D\uDCC1 Created input directory: {}", batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                Path localPath = batchDir.resolve(file.getFilename());

                // ✅ Stream-based download — NO memory load
                blobStorageService.downloadFileToLocal(blobUrl, localPath);

                file.setBlobUrl(localPath.toString());
                logger.info("⬇️ Downloaded file {} to local path {}", blobUrl, localPath);
            }

            String url = switch (message.getSourceSystem().toUpperCase()) {
                case "DEBTMAN" -> otOrchestrationApiUrl;
                case "MFC" -> orchestrationMfcUrl;
                default -> throw new IllegalArgumentException("Unsupported source system: " + message.getSourceSystem());
            };

            logger.info("\uD83D\uDE80 Calling Orchestration API: {}", url);
            OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);

            if (otResponse == null) {
                logger.error("❌ OT orchestration failed for batch {}", batchId);
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"OT call failed\"}");
                ack.acknowledge();
                return;
            }

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(Map.of(
                    "batchID", batchId,
                    "status", "PENDING",
                    "message", "OT Request Sent"
            )));
            logger.info("\uD83D\uDCE4 OT request sent for batch {}", batchId);
            ack.acknowledge();
            executor.submit(() -> processAfterOT(message, otResponse));

        } catch (Exception ex) {
            logger.error("❌ Kafka processing failed", ex);
        }
    }
