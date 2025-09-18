package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KafkaListenerService {

    @Value("${kafka.topic.audit}")
    private String auditTopic;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;       // For normal topic
    private final KafkaTemplate<String, String> auditKafkaTemplate;  // For audit topic
    private final SourceSystemProperties sourceSystemProperties;
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    public KafkaListenerService(
            BlobStorageService blobStorageService,
            @Qualifier("kafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
            @Qualifier("auditKafkaTemplate") KafkaTemplate<String, String> auditKafkaTemplate,
            SourceSystemProperties sourceSystemProperties
    ) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
        this.auditKafkaTemplate = auditKafkaTemplate;
        this.sourceSystemProperties = sourceSystemProperties;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        String batchId = "";
        try {
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            batchId = message.getBatchId();
            List<BatchFile> batchFiles = message.getBatchFiles();

            if (batchFiles == null || batchFiles.isEmpty()) {
                logger.error("❌ [batchId: {}] Empty BatchFiles", batchId);
                ack.acknowledge();
                return;
            }

            // Validate DATA / REF files
            long dataCount = batchFiles.stream()
                    .filter(f -> FILE_TYPE_DATA.equalsIgnoreCase(f.getFileType()))
                    .count();
            long refCount = batchFiles.stream()
                    .filter(f -> FILE_TYPE_REF.equalsIgnoreCase(f.getFileType()))
                    .count();

            if (dataCount == 0 || dataCount > 1) {
                logger.error("❌ [batchId: {}] Invalid file combination", batchId);
                ack.acknowledge();
                return;
            }

            // Download files locally
            downloadAndPrepareFiles(batchId, message, batchFiles);

            // ✅ Send INBOUND audit asynchronously
            Instant startTime = Instant.now();
            long customerCount = batchFiles.size();
            AuditMessage inboundAudit = buildAuditMessage(message, startTime, startTime,
                    "FmConsume", "INBOUND", customerCount);
            sendToAuditTopicAsync(inboundAudit);

            // Lookup orchestration
            Optional<SourceSystemProperties.SystemConfig> matchingConfig =
                    sourceSystemProperties.getConfigForSourceSystem(message.getSourceSystem());
            if (matchingConfig.isEmpty()) {
                logger.error("❌ [batchId: {}] Unsupported source system", batchId);
                ack.acknowledge();
                return;
            }

            SourceSystemProperties.SystemConfig config = matchingConfig.get();
            String url = config.getUrl();
            String token = blobStorageService.getSecret(
                    sourceSystemProperties.getSystems().get(0).getToken()
            );

            if (url == null || url.isBlank()) {
                logger.error("❌ [batchId: {}] Orchestration URL not configured", batchId);
                ack.acknowledge();
                return;
            }

            // ✅ Acknowledge Kafka offset before async OT call
            ack.acknowledge();

            executor.submit(() -> {
                Instant otStartTime = Instant.now();
                try {
                    OTResponse otResponse = callOrchestrationBatchApi(token, url, message);
                    processAfterOT(message, otResponse);

                    // ✅ Send OUTBOUND audit asynchronously
                    Instant otEndTime = Instant.now();
                    AuditMessage outboundAudit = buildAuditMessage(message, otStartTime, otEndTime,
                            "FmConsume", "OUTBOUND", customerCount);
                    sendToAuditTopicAsync(outboundAudit);

                } catch (Exception ex) {
                    logger.error("❌ [batchId: {}] Error during async OT: {}", batchId, ex.getMessage(), ex);
                }
            });

        } catch (Exception ex) {
            logger.error("❌ [batchId: {}] Kafka message processing failed: {}", batchId, ex.getMessage(), ex);
            ack.acknowledge();
        }
    }

    /**
     * Sends audit message asynchronously to prevent blocking listener thread.
     */
    private void sendToAuditTopicAsync(AuditMessage auditMessage) {
        try {
            String auditJson = objectMapper.writeValueAsString(auditMessage);
            ListenableFuture<SendResult<String, String>> future =
                    auditKafkaTemplate.send(auditTopic, auditMessage.getBatchId(), auditJson);

            future.addCallback(
                    success -> logger.info("📣 Audit message sent for batchId {}: {}", auditMessage.getBatchId(), auditJson),
                    failure -> logger.error("❌ Failed to send audit message for batchId {}: {}", auditMessage.getBatchId(), failure.getMessage(), failure)
            );

        } catch (JsonProcessingException e) {
            logger.error("❌ Failed to serialize audit message for batchId {}: {}", auditMessage.getBatchId(), e.getMessage(), e);
        }
    }

    // Keep your existing downloadAndPrepareFiles, processAfterOT, buildAuditMessage, callOrchestrationBatchApi methods intact
}
