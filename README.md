public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
    try {
        logger.info("📩 [batchId: unknown] Received Kafka message: {}", rawMessage);
        KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
        String batchId = message.getBatchId();

        List<BatchFile> batchFiles = message.getBatchFiles();
        if (batchFiles == null || batchFiles.isEmpty()) {
            logger.error("❌ [batchId: {}] Rejected - Empty BatchFiles", batchId);
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
            logger.info("✅ [batchId: {}] Valid with 1 DATA file", batchId);
        }
        // 2. Multiple DATA ❌
        else if (dataCount > 1) {
            logger.error("❌ [batchId: {}] Rejected - Multiple DATA files", batchId);
            ack.acknowledge();
            return;
        }
        // 3. REF only ❌
        else if (dataCount == 0 && refCount > 0) {
            logger.error("❌ [batchId: {}] Rejected - Only REF files", batchId);
            ack.acknowledge();
            return;
        }
        // ✅ 4. DATA + REF — pass both to OT
        else if (dataCount == 1 && refCount > 0) {
            logger.info("✅ [batchId: {}] Valid with DATA + REF files (both will be passed to OT)", batchId);
            message.setBatchFiles(batchFiles);
        }
        // 5. Unknown or empty file types ❌
        else {
            logger.error("❌ [batchId: {}] Rejected - Invalid or unsupported file type combination", batchId);
            ack.acknowledge();
            return;
        }

        String sanitizedBatchId = batchId.replaceAll("[^a-zA-Z0-9_-]", "_");
        String sanitizedSourceSystem = message.getSourceSystem().replaceAll("[^a-zA-Z0-9_-]", "_");

        Path batchDir = Paths.get(mountPath, "input", sanitizedSourceSystem, sanitizedBatchId);
        if (Files.exists(batchDir)) {
            logger.warn("⚠️ [batchId: {}] Directory already exists at path: {}", batchId, batchDir);
        } else {
            Files.createDirectories(batchDir);
            logger.info("📁 [batchId: {}] Created input directory: {}", batchId, batchDir);
        }

        for (BatchFile file : message.getBatchFiles()) {
            String blobUrl = file.getBlobUrl();
            Path localPath = batchDir.resolve(file.getFilename());
            blobStorageService.downloadFileToLocal(blobUrl, localPath);
            file.setBlobUrl(localPath.toString());
            logger.info("⬇️ [batchId: {}] Downloaded file: {} to {}", batchId, blobUrl, localPath);
        }

        String sourceSystem = message.getSourceSystem();

        if ("DEBTMAN".equalsIgnoreCase(sourceSystem) && (otOrchestrationApiUrl == null || otOrchestrationApiUrl.isBlank())) {
            logger.error("❌ [batchId: {}] otOrchestrationApiUrl is not configured for 'DEBTMAN'", batchId);
            throw new IllegalArgumentException("otOrchestrationApiUrl is not configured");
        }

        if ("MFC".equalsIgnoreCase(sourceSystem) && (orchestrationMfcUrl == null || orchestrationMfcUrl.isBlank())) {
            logger.error("❌ [batchId: {}] orchestrationMfcUrl is not configured for 'MFC'", batchId);
            throw new IllegalArgumentException("orchestrationMfcUrl is not configured");
        }

        String url = switch (sourceSystem.toUpperCase()) {
            case "DEBTMAN" -> otOrchestrationApiUrl;
            case "MFC" -> orchestrationMfcUrl;
            default -> {
                logger.error("❌ [batchId: {}] Unsupported source system '{}'", batchId, sourceSystem);
                throw new IllegalArgumentException("Unsupported source system: " + sourceSystem);
            }
        };

        if (url == null || url.isBlank()) {
            logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sourceSystem);
            kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"URL not configured\"}");
            ack.acknowledge();
            return;
        }

        logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", batchId, url);
        OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);
        kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(Map.of(
                "batchID", batchId,
                "status", "PENDING",
                "message", "OT Request Sent"
        )));
        logger.info("📤 [batchId: {}] OT request sent successfully", batchId);
        ack.acknowledge();

        executor.submit(() -> processAfterOT(message, otResponse));

    } catch (Exception ex) {
        logger.error("❌ [batchId: unknown] Kafka processing failed", ex);
    }
}
