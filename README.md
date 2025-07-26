public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
    try {
        logger.info("\uD83D\uDCE5 Received Kafka message: {}", rawMessage);
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

        // ✅ Create input folder
        Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
        Files.createDirectories(batchDir);
        logger.info("\uD83D\uDCC1 Created input directory: {}", batchDir);

        for (BatchFile file : message.getBatchFiles()) {
            String blobUrl = file.getBlobUrl();
            Path localPath = batchDir.resolve(file.getFilename());

            // ✅ Stream-based download
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
