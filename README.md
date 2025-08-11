# ===== Audit topic =====
kafka.topic.audit=log-ecp-batch-audit

# ===== Audit Kafka Producer Config =====
spring.kafka.producer.bootstrap-servers=XETELPZKA01.africa.nedcor.net:9093,XETELPKA02.africa.nedcor.net:9093,XETELPKA03.africa.nedcor.net:9093
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

# ===== SSL Settings =====
spring.kafka.producer.security.protocol=SSL
spring.kafka.producer.ssl.truststore-location=file:/app/certs/kafka.truststore.jks
spring.kafka.producer.ssl.truststore-password=changeit
spring.kafka.producer.ssl.keystore-location=file:/app/certs/kafka.keystore.jks
spring.kafka.producer.ssl.keystore-password=changeit
spring.kafka.producer.ssl.key-password=changeit

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> configProps = new HashMap<>(kafkaProperties.buildProducerProperties());
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}

@Autowired
private KafkaTemplate<String, String> kafkaTemplate; // Using your existing Kafka config

@Value("${kafka.topic.audit}")
private String auditTopic; // log-ecp-batch-audit

@KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
    String batchId = "";
    try {
        logger.info("📩 [batchId: unknown] Received Kafka message: {}", rawMessage);
        KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
        batchId = message.getBatchId();

        // 🆕 Publish audit log - message received
        publishAuditEvent(batchId, "RECEIVED", "Kafka message received for processing");

        List<BatchFile> batchFiles = message.getBatchFiles();
        if (batchFiles == null || batchFiles.isEmpty()) {
            logger.error("❌ [batchId: {}] Rejected - Empty BatchFiles", batchId);
            publishAuditEvent(batchId, "REJECTED", "Empty BatchFiles");
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
            publishAuditEvent(batchId, "REJECTED", "Multiple DATA files");
            ack.acknowledge();
            return;
        } else if (dataCount == 0 && refCount > 0) {
            logger.error("❌ [batchId: {}] Rejected - Only REF files", batchId);
            publishAuditEvent(batchId, "REJECTED", "Only REF files");
            ack.acknowledge();
            return;
        } else if (dataCount == 1 && refCount > 0) {
            logger.info("✅ [batchId: {}] Valid with DATA + REF files (both will be passed to OT)", batchId);
            message.setBatchFiles(batchFiles);
        } else {
            logger.error("❌ [batchId: {}] Rejected - Invalid or unsupported file type combination", batchId);
            publishAuditEvent(batchId, "REJECTED", "Invalid file type combination");
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
                publishAuditEvent(batchId, "FAILED", "Failed to clean existing directory");
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
                    publishAuditEvent(batchId, "FAILED", "File missing after download: " + file.getFilename());
                    throw new IOException("Download failed for: " + localPath);
                }

                file.setBlobUrl(localPath.toString());
                logger.info("⬇️ [batchId: {}] Downloaded file: {} to {}", batchId, blobUrl, localPath);
            } catch (Exception e) {
                logger.error("❌ [batchId: {}] Failed to download or overwrite file: {} - {}", batchId, blobUrl, e.getMessage(), e);
                publishAuditEvent(batchId, "FAILED", "Error downloading file: " + file.getFilename());
                throw e;
            }
        }

        // 🔁 Dynamic lookup
        String sourceSystem = message.getSourceSystem();
        Optional<SourceSystemProperties.SystemConfig> matchingConfig =
                sourceSystemProperties.getConfigForSourceSystem(sourceSystem);

        if (matchingConfig.isEmpty()) {
            logger.error("❌ [batchId: {}] Unsupported or unconfigured source system '{}'", batchId, sourceSystem);
            publishAuditEvent(batchId, "REJECTED", "Unsupported source system: " + sourceSystem);
            ack.acknowledge();
            return;
        }

        SourceSystemProperties.SystemConfig config = matchingConfig.get();
        String url = config.getUrl();
        String token = config.getToken();

        if (url == null || url.isBlank()) {
            logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sourceSystem);
            publishAuditEvent(batchId, "FAILED", "Orchestration URL not configured");
            ack.acknowledge();
            return;
        }

        ack.acknowledge();

        String finalBatchId = batchId;
        executor.submit(() -> {
            try {
                logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", finalBatchId, url);
                OTResponse otResponse = callOrchestrationBatchApi(token, url, message);
                logger.info("📤 [batchId: {}] OT request sent successfully", finalBatchId);
                publishAuditEvent(finalBatchId, "SUCCESS", "OT request sent successfully");
                processAfterOT(message, otResponse);
            } catch (Exception ex) {
                logger.error("❌ [batchId: {}] Error during async OT or post-processing: {}", finalBatchId, ex.getMessage(), ex);
                publishAuditEvent(finalBatchId, "FAILED", "Error during OT call: " + ex.getMessage());
            }
        });

    } catch (Exception ex) {
        logger.error("❌ [batchId: {}] Kafka message processing failed. Error: {}", batchId, ex.getMessage(), ex);
        publishAuditEvent(batchId, "FAILED", "Kafka message processing failed: " + ex.getMessage());
        ack.acknowledge();
    }
}

/**
 * Publishes an audit event to the audit Kafka topic
 */
private void publishAuditEvent(String batchId, String status, String description) {
    try {
        Map<String, Object> auditMessage = new HashMap<>();
        auditMessage.put("batchId", batchId);
        auditMessage.put("status", status);
        auditMessage.put("description", description);
        auditMessage.put("timestamp", Instant.now().toString());

        kafkaTemplate.send(auditTopic, batchId, objectMapper.writeValueAsString(auditMessage));
        logger.info("📝 [batchId: {}] Audit event published: {} - {}", batchId, status, description);
    } catch (Exception e) {
        logger.error("⚠️ [batchId: {}] Failed to publish audit event: {}", batchId, e.getMessage(), e);
    }
}
