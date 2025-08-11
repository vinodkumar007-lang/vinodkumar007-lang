Topic Name: log-ecp-batch-audit
 
Bootstrap Servers (ETE):
XETELPZKA01.africa.nedcor.net
	
10.58.150.57


XETELPKA02.africa.nedcor.net
	
10.58.150.58


XETELPKA03.africa.nedcor.net
	
10.58.150.59
 
Port: 9093

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

            // 🔁 New logic starts here — dynamic lookup
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
            String token = config.getToken();

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
                    OTResponse otResponse = callOrchestrationBatchApi(token, url, message);
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
=======================

package com.nedbank.kafka.filemanage.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class AuditEventPublisher {

    private final KafkaProducer<String, String> producer;
    private final String topicName;

    public AuditEventPublisher(String topicName) {
        this.topicName = topicName;
        this.producer = new KafkaProducer<>(createProducerProperties());
    }

    private Properties createProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "nbpigelpdev02.africa.nedcor.net:9093,nbpproelpdev01.africa.nedcor.net:9093,nbpinelpdev01.africa.nedcor.net:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // SSL configuration
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
        props.put("ssl.truststore.password", "nedbank1");
        props.put("ssl.keystore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
        props.put("ssl.keystore.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.key.password", "3dX7y3Yz9Jv6L4F");

        // Optional: better reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        return props;
    }

    public void publishEvent(String key, String message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, message);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(); // blocking send
            System.out.printf("✅ Sent message to %s | partition=%d | offset=%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            System.err.println("❌ Failed to publish message: " + e.getMessage());
        }
    }

    public void close() {
        producer.close();
    }

    // Example usage
    public static void main(String[] args) {
        AuditEventPublisher publisher = new AuditEventPublisher("log-ecp-batch-audit");
        publisher.publishEvent("testKey", "{\"event\":\"FileProcessed\",\"status\":\"SUCCESS\"}");
        publisher.close();
    }
}


