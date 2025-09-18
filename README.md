private void sendToAuditTopic(AuditMessage auditMessage) {
        try {
            String auditJson = objectMapper.writeValueAsString(auditMessage);

            CompletableFuture<SendResult<String, String>> future =
                    auditKafkaTemplate.send(auditTopic, auditMessage.getBatchId(), auditJson);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.error("❌ Failed to send audit message for batchId {}: {}", auditMessage.getBatchId(), ex.getMessage(), ex);
                } else {
                    logger.info("📣 Audit message sent successfully for batchId {}: {}", auditMessage.getBatchId(), auditJson);
                }
            });

        } catch (JsonProcessingException e) {
            logger.error("❌ Failed to serialize audit message for batchId {}: {}", auditMessage.getBatchId(), e.getMessage(), e);
        }
    }

 
 @Value("${kafka.topic.audit}")
    private String auditTopic;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;       // For regular topic
    private final KafkaTemplate<String, String> auditKafkaTemplate;  // For audit topic
    private final SourceSystemProperties sourceSystemProperties;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    public KafkaListenerService(
            BlobStorageService blobStorageService,
            @Qualifier("kafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,                       // default template
            @Qualifier("auditKafkaTemplate") KafkaTemplate<String, String> auditKafkaTemplate,  // audit template
            SourceSystemProperties sourceSystemProperties
    ) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
        this.auditKafkaTemplate = auditKafkaTemplate;
        this.sourceSystemProperties = sourceSystemProperties;
    }

    /**
     * Kafka consumer method to handle messages from input topic.
     * Performs validation on message structure, downloads files,
     * and triggers orchestration API.
     *
     * @param rawMessage Raw Kafka message in JSON string format
     * @param ack        Kafka acknowledgment to commit offset manually
     */
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

                    // Download file locally
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

            // Send INBOUND audit after files are ready
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
                ack.acknowledge();
                return;
            }

            SourceSystemProperties.SystemConfig config = matchingConfig.get();
            String url = config.getUrl();
            String secretName = sourceSystemProperties.getSystems().get(0).getToken();
            String token = blobStorageService.getSecret(secretName);

            if (url == null || url.isBlank()) {
                logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sanitizedSourceSystem);
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

 package com.nedbank.kafka.filemanage.config;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Audit Kafka Producer Configuration with SSL settings.
 * Used to send audit events securely to Kafka.
 */
@Configuration
public class KafkaAuditProducerConfig {

    @Value("${audit.kafka.producer.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${audit.kafka.producer.security.protocol}")
    private String securityProtocol;

    @Value("${audit.kafka.producer.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${audit.kafka.producer.ssl.truststore.password}")
    private String truststorePassword;

    @Value("${audit.kafka.producer.ssl.keystore.location}")
    private String keystoreLocation;

    @Value("${audit.kafka.producer.ssl.keystore.password}")
    private String keystorePassword;

    @Value("${audit.kafka.producer.ssl.key.password}")
    private String keyPassword;

    @Value("${audit.kafka.producer.ssl.protocol}")
    private String sslProtocol;

    @PostConstruct
    public void logAuditKafkaProps() {
        System.out.println("========== AUDIT KAFKA PRODUCER CONFIG ==========");
        System.out.println("Bootstrap Servers : " + bootstrapServers);
        System.out.println("Security Protocol : " + securityProtocol);
        System.out.println("Truststore Location : " + truststoreLocation);
        System.out.println("truststore Password   : " + truststorePassword);
        System.out.println("keystore Location : " + keystoreLocation);
        System.out.println("Keystore Password   : " + keystorePassword);
        System.out.println("keyPassword   : " + keyPassword);
        System.out.println("sslProtocol             : " + sslProtocol);
        System.out.println("=================================================");
    }

    @Bean(name = "auditProducerFactory")
    public ProducerFactory<String, String> auditProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // SSL Security configs
        configProps.put("security.protocol", securityProtocol);
        configProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        configProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        configProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        configProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        configProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        configProps.put(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);

        // Reliability settings
        configProps.put(ProducerConfig.RETRIES_CONFIG, 5);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 180000);
        configProps.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 30000);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean(name = "auditKafkaTemplate")
    public KafkaTemplate<String, String> auditKafkaTemplate() {
        return new KafkaTemplate<>(auditProducerFactory());
    }
}

package com.nedbank.kafka.filemanage.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for setting up Kafka producer with SSL security.
 * This configuration enables Kafka message publishing with secure connectivity using keystore and truststore credentials.
 */
@Configuration
public class KafkaProducerConfig {

    // Kafka broker address (e.g., host:port)
    @Value("${kafka.producer.bootstrap.servers}")
    private String bootstrapServers;

    // Security protocol (e.g., SSL)
    @Value("${kafka.producer.security.protocol}")
    private String securityProtocol;

    // Truststore configuration
    @Value("${kafka.producer.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${kafka.producer.ssl.truststore.password}")
    private String truststorePassword;

    // Keystore configuration
    @Value("${kafka.producer.ssl.keystore.location}")
    private String keystoreLocation;

    @Value("${kafka.producer.ssl.keystore.password}")
    private String keystorePassword;

    // Key password for producer's private key
    @Value("${kafka.producer.ssl.key.password}")
    private String keyPassword;

    // SSL Protocol (e.g., TLSv1.2)
    @Value("${kafka.producer.ssl.protocol}")
    private String sslProtocol;

    /**
     * Configures and provides a Kafka ProducerFactory with SSL settings.
     *
     * @return ProducerFactory for producing Kafka messages.
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Basic producer settings
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // SSL Security configurations
        configProps.put("security.protocol", securityProtocol);
        configProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        configProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        configProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        configProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        configProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        configProps.put(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);
        // Recommended tuning options for reliability and timeout handling
        configProps.put(ProducerConfig.RETRIES_CONFIG, 5);                          // Retry up to 5 times
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);          // Request timeout
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 180000);               // Max block while creating producer
        configProps.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 30000);            // Metadata refresh interval
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);             // Backoff between retries

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Provides a KafkaTemplate for sending messages to Kafka.
     *
     * @return KafkaTemplate instance wired to the configured producer factory.
     */
    @Bean(name = "kafkaTemplate")
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}

