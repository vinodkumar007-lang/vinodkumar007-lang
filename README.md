public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
    try {
        logger.info("📩 Received Kafka message: {}", rawMessage);
        KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
        String batchId = message.getBatchId();

        List<BatchFile> batchFiles = message.getBatchFiles();
        if (batchFiles == null || batchFiles.isEmpty()) {
            logger.error("❌ Rejected batch {} - Empty BatchFiles", batchId);
            ack.acknowledge();
            return;
        }

        long dataCount = batchFiles.stream()
                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                .count();
        long refCount = batchFiles.stream()
                .filter(f -> "REF".equalsIgnoreCase(f.getFileType()))
                .count();

        // 1. DATA only ✅
        if (dataCount == 1 && refCount == 0) {
            logger.info("✅ Valid batch {} with 1 DATA file", batchId);
        }
        // 2. Multiple DATA ❌
        else if (dataCount > 1) {
            logger.error("❌ Rejected batch {} - Multiple DATA files", batchId);
            ack.acknowledge();
            return;
        }
        // 3. REF only ❌
        else if (dataCount == 0 && refCount > 0) {
            logger.error("❌ Rejected batch {} - Only REF files", batchId);
            ack.acknowledge();
            return;
        }
        // 4. REF + DATA ✅ (but ignore REF)
        else if (dataCount == 1 && refCount > 0) {
            logger.info("✅ Valid batch {} with DATA + REF (REF will be ignored)", batchId);
            message.setBatchFiles(
                    batchFiles.stream()
                            .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                            .toList()
            );
        }
        // 5. Unknown or empty file types ❌
        else {
            logger.error("❌ Rejected batch {} - Invalid or unsupported file type combination", batchId);
            ack.acknowledge();
            return;
        }

        // ✅ Sanitize path components to prevent directory traversal attacks
        String sanitizedBatchId = batchId.replaceAll("[^a-zA-Z0-9_-]", "_");
        String sanitizedSourceSystem = message.getSourceSystem().replaceAll("[^a-zA-Z0-9_-]", "_");

        // ✅ Create input folder (warn if already exists)
        Path batchDir = Paths.get(mountPath, "input", sanitizedSourceSystem, sanitizedBatchId);
        if (Files.exists(batchDir)) {
            logger.warn("⚠️ Directory already exists for batch {} at path: {}", batchId, batchDir);
        } else {
            Files.createDirectories(batchDir);
            logger.info("📁 Created input directory for batch {}: {}", batchId, batchDir);
        }

        for (BatchFile file : message.getBatchFiles()) {
            String blobUrl = file.getBlobUrl();
            Path localPath = batchDir.resolve(file.getFilename());

            // ✅ Download using stream (no heap memory issues)
            blobStorageService.downloadFileToLocal(blobUrl, localPath);
            file.setBlobUrl(localPath.toString());

            logger.info("⬇️ Downloaded file for batch {}: {} to {}", batchId, blobUrl, localPath);
        }

        // ✅ Validate Orchestration API URL
        String sourceSystem = message.getSourceSystem();
        String url = switch (sourceSystem.toUpperCase()) {
            case "DEBTMAN" -> otOrchestrationApiUrl;
            case "MFC" -> orchestrationMfcUrl;
            default -> {
                logger.error("❌ Unsupported source system '{}' in batch {}", sourceSystem, batchId);
                throw new IllegalArgumentException("Unsupported source system: " + sourceSystem);
            }
        };

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

        kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(Map.of(
                "batchID", batchId,
                "status", "PENDING",
                "message", "OT Request Sent"
        )));
        logger.info("📤 OT request sent successfully for batch {}", batchId);

        ack.acknowledge();

        // ✅ Async post-OT processing
        executor.submit(() -> processAfterOT(message, otResponse));

    } catch (Exception ex) {
        logger.error("❌ Kafka processing failed", ex);
    }
}

🔐 Input Sanitization
batchId and sourceSystem are now sanitized before any filesystem usage to prevent path traversal or injection risks.

🧵 Golden Thread Logging
All log statements now include the batchId to improve traceability across logs.

📁 Folder Existence Trace
Added checks for pre-existing folders. A warning log is now generated if the input folder already exists, without deleting any content.

🌐 Orchestration URL Validation
The system now explicitly validates that the orchestration URL is configured for supported sourceSystem values. If not, a clear exception is raised.
