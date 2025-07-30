public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
    ObjectMapper objectMapper = new ObjectMapper();
    Message message;
    try {
        message = objectMapper.readValue(rawMessage, Message.class);
    } catch (Exception ex) {
        logger.error("❌ [UNKNOWN] Failed to parse Kafka message: {}", ex.getMessage());
        ack.acknowledge();
        return;
    }

    String batchId = message.getBatchId();
    logger.info("[batchId: {}] 📩 Received Kafka message: {}", batchId, rawMessage);

    List<FileMeta> batchFiles = message.getBatchFiles();
    if (batchFiles == null || batchFiles.isEmpty()) {
        logger.error("[batchId: {}] ❌ Rejected - Empty BatchFiles", batchId);
        ack.acknowledge();
        return;
    }

    long dataCount = batchFiles.stream().filter(f -> "DATA".equalsIgnoreCase(f.getFileType())).count();
    long refCount = batchFiles.stream().filter(f -> "REF".equalsIgnoreCase(f.getFileType())).count();

    if (dataCount == 1 && refCount == 0) {
        logger.info("[batchId: {}] ✅ Valid with 1 DATA file", batchId);
    } else if (dataCount > 1) {
        logger.error("[batchId: {}] ❌ Rejected - Multiple DATA files", batchId);
        ack.acknowledge();
        return;
    } else if (dataCount == 0 && refCount >= 1) {
        logger.error("[batchId: {}] ❌ Rejected - Only REF files", batchId);
        ack.acknowledge();
        return;
    } else if (dataCount == 1 && refCount >= 1) {
        logger.info("[batchId: {}] ✅ Valid with DATA + REF (sending to OT)", batchId);
    } else {
        logger.error("[batchId: {}] ❌ Rejected - Unsupported file combination", batchId);
        ack.acknowledge();
        return;
    }

    try {
        // ✅ Sanitize path components
        String sanitizedBatchId = batchId.replaceAll("[^a-zA-Z0-9_-]", "_");
        String sanitizedSourceSystem = message.getSourceSystem().replaceAll("[^a-zA-Z0-9_-]", "_");
        Path batchDir = Path.of(baseInputDir, sanitizedSourceSystem, sanitizedBatchId);

        if (Files.exists(batchDir)) {
            logger.warn("[batchId: {}] ⚠️ Directory already exists at: {}", batchId, batchDir);
        } else {
            Files.createDirectories(batchDir);
            logger.info("[batchId: {}] 📁 Created input directory at: {}", batchId, batchDir);
        }

        for (FileMeta file : batchFiles) {
            String blobUrl = file.getBlobUrl();
            String fileName = file.getFileName();
            Path localPath = batchDir.resolve(fileName);
            blobStorageService.downloadFileContent(blobUrl, localPath);
            logger.info("[batchId: {}] ⬇️ Downloaded file: {} → {}", batchId, blobUrl, localPath);
        }

        // ✅ URL resolution with null/blank check
        String sourceSystem = message.getSourceSystem();
        String url = switch (sourceSystem.toUpperCase()) {
            case "DEBTMAN" -> {
                if (otOrchestrationApiUrl == null || otOrchestrationApiUrl.isBlank()) {
                    logger.error("[batchId: {}] ❌ otOrchestrationApiUrl not configured for DEBTMAN", batchId);
                    ack.acknowledge();
                    return;
                }
                yield otOrchestrationApiUrl;
            }
            case "MFC" -> {
                if (orchestrationMfcUrl == null || orchestrationMfcUrl.isBlank()) {
                    logger.error("[batchId: {}] ❌ orchestrationMfcUrl not configured for MFC", batchId);
                    ack.acknowledge();
                    return;
                }
                yield orchestrationMfcUrl;
            }
            default -> {
                logger.error("[batchId: {}] ❌ Unsupported source system '{}'", batchId, sourceSystem);
                throw new IllegalArgumentException("Unsupported source system: " + sourceSystem);
            }
        };

        if (url == null || url.isBlank()) {
            logger.error("[batchId: {}] ❌ Orchestration URL is blank/null for source system '{}'", batchId, sourceSystem);
            ack.acknowledge();
            return;
        }

        logger.info("[batchId: {}] 🚀 Calling Orchestration API at: {}", batchId, url);

        // Replace with actual OT processing logic
        boolean otSuccess = orchestrationService.processBatch(message, url);
        if (otSuccess) {
            logger.info("[batchId: {}] 📤 OT request sent successfully", batchId);
        } else {
            logger.error("[batchId: {}] ❌ OT processing failed", batchId);
        }

    } catch (Exception ex) {
        logger.error("[batchId: {}] ❌ Kafka processing failed: {}", batchId, ex.getMessage(), ex);
    } finally {
        ack.acknowledge();
    }
}
