// --------------------- KafkaListener Method ---------------------
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

        // Download and upload files
        for (BatchFile file : message.getBatchFiles()) {
            String blobUrl = file.getBlobUrl();
            Path localPath = batchDir.resolve(file.getFilename());

            try {
                if (Files.exists(localPath)) {
                    logger.warn("♻️ [batchId: {}] File already exists, overwriting: {}", batchId, localPath);
                    Files.delete(localPath);
                }

                // Download file locally
                blobStorageService.downloadFileToLocal(blobUrl, localPath);

                if (!Files.exists(localPath)) {
                    logger.error("❌ [batchId: {}] File missing after download: {}", batchId, localPath);
                    throw new IOException("Download failed for: " + localPath);
                }

                // Upload to Azure and set blobUrl in batchFile
                String uploadedBlobUrl = blobStorageService.uploadFileAndReturnLocation(
                        localPath, file.getFilename(), message.getSourceSystem(), batchId
                );
                file.setBlobUrl(uploadedBlobUrl);
                logger.info("⬆️ [batchId: {}] Uploaded file to Azure blob: {}", batchId, uploadedBlobUrl);

            } catch (Exception e) {
                logger.error("❌ [batchId: {}] Failed to download or upload file: {} - {}", batchId, file.getFilename(), e.getMessage(), e);
                throw e;
            }
        }

        // Send INBOUND audit after files are ready
        Instant startTime = Instant.now();
        long customerCount = message.getBatchFiles().size(); // or your FM logic
        AuditMessage inboundAudit = buildAuditMessage(message, startTime, startTime,
                                                      "FmConsume", "INBOUND", customerCount);
        sendToAuditTopic(inboundAudit);

        // Dynamic lookup for orchestration
        String sourceSystem = message.getSourceSystem();
        Optional<SourceSystemProperties.SystemConfig> matchingConfig =
                sourceSystemProperties.getConfigForSourceSystem(sourceSystem);

        if (matchingConfig.isEmpty()) {
            logger.error("❌ [batchId: {}] Unsupported or unconfigured source system '{}'", batchId, sourceSystem);
            ack.acknowledge();
            return;
        }

        SourceSystemProperties.SystemConfig config = matchingConfig.get();
        String url = config.getUrl();
        String secretName = sourceSystemProperties.getSystems().get(0).getToken();
        String token = blobStorageService.getSecret(secretName);

        if (url == null || url.isBlank()) {
            logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sourceSystem);
            ack.acknowledge();
            return;
        }

        // ✅ Acknowledge before async OT call
        ack.acknowledge();

        String finalBatchId = batchId;
        executor.submit(() -> {
            Instant otStartTime = Instant.now();
            try {
                logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", finalBatchId, url);
                OTResponse otResponse = callOrchestrationBatchApi(token, url, message);
                logger.info("📤 [batchId: {}] OT request sent successfully", finalBatchId);
                processAfterOT(message, otResponse);

                // Send OUTBOUND audit
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

// ---------------------- Helper Methods ----------------------
private AuditMessage buildAuditMessage(KafkaMessage message,
                                       Instant startTime,
                                       Instant endTime,
                                       String serviceName,
                                       String eventType,
                                       long customerCount) {
    AuditMessage audit = new AuditMessage();
    audit.setBatchId(message.getBatchId());
    audit.setServiceName(serviceName);
    audit.setSystemEnv(systemEnv); // DEV/QA/PROD
    audit.setSourceSystem(message.getSourceSystem());
    audit.setTenantCode(message.getTenantCode());
    audit.setChannelID(message.getChannelID());
    audit.setProduct(message.getProduct());
    audit.setJobName(message.getJobName());
    audit.setUniqueConsumerRef(message.getUniqueConsumerRef());
    audit.setTimestamp(Instant.now().toString());
    audit.setRunPriority(message.getRunPriority());
    audit.setEventType(eventType);
    audit.setStartTime(startTime.toString());
    audit.setEndTime(endTime.toString());
    audit.setCustomerCount(customerCount);

    List<AuditBatchFile> auditFiles = message.getBatchFiles().stream()
            .map(f -> new AuditBatchFile(f.getBlobUrl(), f.getFilename(), f.getFileType()))
            .toList();
    audit.setBatchFiles(auditFiles);
    return audit;
}

private void sendToAuditTopic(AuditMessage auditMessage) {
    try {
        String auditJson = objectMapper.writeValueAsString(auditMessage);
        kafkaTemplate.send(auditTopic, auditMessage.getBatchId(), auditJson);
        logger.info("📣 Audit message sent for batchId {}: {}", auditMessage.getBatchId(), auditJson);
    } catch (JsonProcessingException e) {
        logger.error("❌ Failed to serialize audit message for batchId {}: {}", auditMessage.getBatchId(), e.getMessage(), e);
    }
}

// ---------------------- Audit DTOs ----------------------
@Data
@AllArgsConstructor
@NoArgsConstructor
public static class AuditBatchFile {
    private String blobUrl;
    private String fileName;
    private String fileType;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
public static class AuditMessage {
    private String batchId;
    private String serviceName;
    private String systemEnv;
    private String sourceSystem;
    private String tenantCode;
    private String channelID;
    private String product;
    private String jobName;
    private String uniqueConsumerRef;
    private String timestamp;
    private String runPriority;
    private String eventType;
    private String startTime;
    private String endTime;
    private long customerCount;
    private List<AuditBatchFile> batchFiles;
}

// ---------------------- Required @Bean in Configuration ----------------------
@Bean
public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
}

@Bean
public String auditTopic() {
    return environment.getProperty("kafka.topic.audit", "audit-topic");
}

@Value("${kafka.topic.audit}")
private String auditTopic;
