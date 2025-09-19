@KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
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

        // Download files
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
                logger.error("❌ [batchId: {}] Failed to download file: {} - {}", batchId, file.getFilename(), e.getMessage(), e);
                throw e;
            }
        }

        // ✅ Commit main topic early (before sending INBOUND audit)
        ack.acknowledge();

        // Send INBOUND audit after commit
        Instant startTime = Instant.now();
        long customerCount = message.getBatchFiles().size();
        AuditMessage inboundAudit = buildAuditMessage(message, startTime, startTime,
                "FmConsume", "INBOUND", customerCount);
        sendToAuditTopic(inboundAudit);

        // Dynamic lookup for orchestration
        Optional<SourceSystemProperties.SystemConfig> matchingConfig =
                sourceSystemProperties.getConfigForSourceSystem(sanitizedSourceSystem);

        if (matchingConfig.isEmpty()) {
            logger.error("❌ [batchId: {}] Unsupported or unconfigured source system '{}'", batchId, sanitizedSourceSystem);
            return;
        }

        SourceSystemProperties.SystemConfig config = matchingConfig.get();
        String url = config.getUrl();
        String secretName = sourceSystemProperties.getSystems().get(0).getToken();
        String token = blobStorageService.getSecret(secretName);

        if (url == null || url.isBlank()) {
            logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sanitizedSourceSystem);
            return;
        }

        String finalBatchId = batchId;
        executor.submit(() -> {
            Instant otStartTime = Instant.now();
            try {
                logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", finalBatchId, url);
                OTResponse otResponse = callOrchestrationBatchApi(token, url, message);
                logger.info("📤 [batchId: {}] OT request sent successfully", finalBatchId);
                processAfterOT(message, otResponse);

                // Send OUTBOUND audit (commit is already done earlier)
                Instant otEndTime = Instant.now();
                AuditMessage outboundAudit = buildAuditMessage(message, otStartTime, otEndTime,
                        "FmConsume", "OUTBOUND", customerCount);
                sendToAuditTopic(outboundAudit);

            } catch (Exception ex) {
                logger.error("❌ [batchId: {}] Error during async OT or post-processing: {}", finalBatchId, ex.getMessage(), ex);
            }
        });

    } catch (Exception ex) {
        logger.error("❌ [batchId: {}] Kafka message processing failed. Error: {}", batchId, ex.getMessage(), ex);
        ack.acknowledge();
    }
}
