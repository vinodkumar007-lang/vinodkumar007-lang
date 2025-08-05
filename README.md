public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
    String batchId = "";
    try {
        logger.info("📩 [batchId: unknown] Received Kafka message: {}", rawMessage);
        KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
        batchId = message.getBatchId();
        List<BatchFile> batchFiles = message.getBatchFiles();
        if (batchFiles == null || batchFiles.isEmpty()) {
            logger.error("❌ [batchId: {}] Rejected - Empty BatchFiles", batchId);
            ack.acknowledge();
            return;
        }

        long dataCount = batchFiles.stream()
                .filter(f -> FILE_TYPE_DATA.equalsIgnoreCase(f.getFileType()))
                .count();
        long refCount = batchFiles.stream()
                .filter(f -> FILE_TYPE_REF.equalsIgnoreCase(f.getFileType()))
                .count();

        if (dataCount == 1 && refCount == 0) {
            logger.info("✅ [batchId: {}] Valid with 1 DATA file", batchId);
        } else if (dataCount > 1) {
            logger.error("❌ [batchId: {}] Rejected - Multiple DATA files", batchId);
            ack.acknowledge();
            return;
        } else if (dataCount == 0 && refCount > 0) {
            logger.error("❌ [batchId: {}] Rejected - Only REF files", batchId);
            ack.acknowledge();
            return;
        } else if (dataCount == 1 && refCount > 0) {
            logger.info("✅ [batchId: {}] Valid with DATA + REF files (both will be passed to OT)", batchId);
            message.setBatchFiles(batchFiles);
        } else {
            logger.error("❌ [batchId: {}] Rejected - Invalid or unsupported file type combination", batchId);
            ack.acknowledge();
            return;
        }

        String sanitizedBatchId = batchId.replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);
        String sanitizedSourceSystem = message.getSourceSystem().replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);

        Path batchDir = Paths.get(mountPath, INPUT_FOLDER, sanitizedSourceSystem, sanitizedBatchId);
        if (Files.exists(batchDir)) {
            logger.warn("⚠️ [batchId: {}] Directory already exists at path: {}", batchId, batchDir);
            try (Stream<Path> files = Files.walk(batchDir)) {
                files.sorted(Comparator.reverseOrder())
                     .map(Path::toFile)
                     .forEach(File::delete);
                logger.info("🧹 [batchId: {}] Cleaned existing input directory: {}", batchId, batchDir);
            } catch (IOException e) {
                logger.error("❌ [batchId: {}] Failed to clean directory {} - {}", batchId, batchDir, e.getMessage(), e);
                throw e;
            }
        }

        Files.createDirectories(batchDir);
        logger.info("📁 [batchId: {}] Created input directory: {}", batchId, batchDir);

        for (BatchFile file : message.getBatchFiles()) {
            String blobUrl = file.getBlobUrl();
            Path localPath = batchDir.resolve(file.getFilename());

            try {
                if (Files.exists(localPath)) {
                    logger.warn("♻️ [batchId: {}] File already exists, overwriting: {}", batchId, localPath);
                    Files.delete(localPath);
                }

                blobStorageService.downloadFileToLocal(blobUrl, localPath);

                if (!Files.exists(localPath)) {
                    logger.error("❌ [batchId: {}] File missing after download: {}", batchId, localPath);
                    throw new IOException("Download failed for: " + localPath);
                }

                file.setBlobUrl(localPath.toString());
                logger.info("⬇️ [batchId: {}] Downloaded file: {} to {}", batchId, blobUrl, localPath);
            } catch (Exception e) {
                logger.error("❌ [batchId: {}] Failed to download or overwrite file: {} - {}", batchId, blobUrl, e.getMessage(), e);
                throw e;
            }
        }

        String sourceSystem = message.getSourceSystem();

        if (SOURCE_DEBTMAN.equalsIgnoreCase(sourceSystem) && (otOrchestrationApiUrl == null || otOrchestrationApiUrl.isBlank())) {
            logger.error("❌ [batchId: {}] otOrchestrationApiUrl is not configured for 'DEBTMAN'", batchId);
            throw new IllegalArgumentException(ERROR_DEBTMAN_URL_NOT_CONFIGURED);
        }

        if (SOURCE_MFC.equalsIgnoreCase(sourceSystem) && (orchestrationMfcUrl == null || orchestrationMfcUrl.isBlank())) {
            logger.error("❌ [batchId: {}] orchestrationMfcUrl is not configured for 'MFC'", batchId);
            throw new IllegalArgumentException(ERROR_MFC_URL_NOT_CONFIGURED);
        }

        String url = switch (sourceSystem.toUpperCase()) {
            case SOURCE_DEBTMAN -> otOrchestrationApiUrl;
            case SOURCE_MFC -> orchestrationMfcUrl;
            default -> {
                logger.error("❌ [batchId: {}] Unsupported source system '{}'", batchId, sourceSystem);
                throw new IllegalArgumentException("Unsupported source system: " + sourceSystem);
            }
        };

        if (url == null || url.isBlank()) {
            logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sourceSystem);
            ack.acknowledge();
            return;
        }

        // ✅ Acknowledge before async OT call
        ack.acknowledge();

        String finalBatchId = batchId;
        executor.submit(() -> {
            try {
                logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", finalBatchId, url);
                OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);
                logger.info("📤 [batchId: {}] OT request sent successfully", finalBatchId);
                processAfterOT(message, otResponse);
            } catch (Exception ex) {
                logger.error("❌ [batchId: {}] Error during async OT or post-processing: {}", finalBatchId, ex.getMessage(), ex);
            }
        });

    } catch (Exception ex) {
        logger.error("❌ [batchId: {}] Kafka message processing failed. Error: {}", batchId, ex.getMessage(), ex);
        ack.acknowledge();
    }
}
