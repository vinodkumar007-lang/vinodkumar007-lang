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
            String jobName = message.getJobName();

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

            // Send Fmcompose audit (INBOUND) with dynamic retryFlag and retryCount false/zero at start
            Instant startTime = Instant.now();
            long customerCount = message.getBatchFiles().stream().mapToLong(BatchFile::getCustomerCount).sum();
            ECPBatchAudit fmcomposeAudit = ECPBatchAudit.builder()
                    .title("ECPBatchAudit")
                    .type("object")
                    .datastreamName("Fmcompose")
                    .datastreamType("logs")
                    .batchId(message.getBatchId())
                    .serviceName("Fmcompose")
                    .systemEnv(systemEnv) // or supply from config
                    .sourceSystem(message.getSourceSystem())
                    .tenantCode(message.getTenantCode())
                    .channelId(message.getChannelID())
                    .audienceId(message.getAudienceID())
                    .product(message.getProduct())
                    .jobName(message.getJobName())
                    .consumerRef(message.getConsumerRef())
                    .timestamp(Instant.now().toString())
                    .eventType(message.getEventType())
                    .startTime(startTime)
                    .endTime(startTime)
                    .customerCount(customerCount)
                    .batchFiles(message.getBatchFiles().stream()
                            .map(f -> new ECPBatchAudit.BatchFileAudit(f.getBlobUrl(), f.getFileName(), f.getFileType()))
                            .collect(Collectors.toList()))
                    .success(true)
                    .retryFlag(false)
                    .retryCount(0)
                    .build();

            sendToAuditTopic(fmcomposeAudit);

            // Download files
            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                Path localPath = batchDir.resolve(file.getFileName());

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
                    logger.error("❌ [batchId: {}] Failed to download file: {} - {}", batchId, file.getFileName(), e.getMessage(), e);
                    throw e;
                }
            }

            // ✅ Commit main topic early
            ack.acknowledge();

            Optional<SourceSystemProperties.SystemConfig> matchingConfig =
                    sourceSystemProperties.getConfigForSourceSystem(sanitizedSourceSystem, jobName);

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
            long finalCustomerCount = customerCount;
            executor.submit(() -> {
                Instant otStartTime = Instant.now();
                boolean retryFlag = false;
                int retryCount = 0;
                OTResponse otResponse = null;
                int maxRetries = 3;

                while (retryCount <= maxRetries) {
                    try {
                        logger.info("🚀 [batchId: {}] Calling Orchestration API attempt {}: {}", finalBatchId, retryCount + 1, url);
                        otResponse = callOrchestrationBatchApi(token, url, message);
                        if (otResponse.isSuccess()) {
                            break;
                        }
                    } catch (Exception e) {
                        logger.warn("⚠️ Orchestration call failed (attempt {}): {}", retryCount + 1, e.getMessage());
                    }
                    retryCount++;
                    if (retryCount > 0) retryFlag = true;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }

                if (otResponse == null) {
                    otResponse = new OTResponse();
                    otResponse.setSuccess(false);
                    otResponse.setMessage("Failed after retries");
                }

                try {
                    SummaryResponse summaryResponse = processAfterOT(message, otResponse);
                    Instant otEndTime = Instant.now();
                    String summaryUrl = summaryResponse.getSummaryFileURL();
                    // Get filename after last "/"
                    String filename = summaryUrl.substring(summaryUrl.lastIndexOf('/') + 1);

                    // Get file extension after last "."
                    String fileType = "";
                    int dotIndex = filename.lastIndexOf('.');
                    if (dotIndex != -1 && dotIndex < filename.length() - 1) {
                        fileType = filename.substring(dotIndex + 1);
                    }

                    // Send Fmcomplete audit with dynamic retry info
                    ECPBatchAudit fmcompleteAudit = ECPBatchAudit.builder()
                            .title("ECPBatchAudit")
                            .type("object")
                            .datastreamName("Fmcomplete")
                            .datastreamType("logs")
                            .batchId(message.getBatchId())
                            .serviceName("Fmcomplete")
                            .systemEnv(systemEnv)
                            .sourceSystem(message.getSourceSystem())
                            .tenantCode(message.getTenantCode())
                            .channelId(message.getChannelID())
                            .audienceId(message.getAudienceID())
                            .product(message.getProduct())
                            .jobName(message.getJobName())
                            .consumerRef(message.getConsumerRef())
                            .timestamp(Instant.now().toString())
                            .eventType(message.getEventType())
                            .startTime(otStartTime)
                            .endTime(otEndTime)
                            .customerCount(finalCustomerCount)
                            .batchFiles(Collections.singletonList(new ECPBatchAudit.BatchFileAudit(summaryUrl, filename, fileType)))
                            .success(otResponse.isSuccess())
                            .retryFlag(retryFlag)
                            .retryCount(retryCount)
                            .build();

                    sendToAuditTopic(fmcompleteAudit);

                } catch (Exception ex) {
                    logger.error("❌ [batchId: {}] Error during async OT or post-processing: {}", finalBatchId, ex.getMessage(), ex);
                    // If error on post-processing send audit with failure info
                    ECPBatchAudit fmcompleteAuditFail = ECPBatchAudit.builder()
                            .title("ECPBatchAudit")
                            .type("object")
                            .datastreamName("Fmcomplete")
                            .datastreamType("logs")
                            .batchId(message.getBatchId())
                            .serviceName("Fmcomplete")
                            .systemEnv(systemEnv)
                            .sourceSystem(message.getSourceSystem())
                            .tenantCode(message.getTenantCode())
                            .channelId(message.getChannelID())
                            .audienceId(message.getAudienceID())
                            .product(message.getProduct())
                            .jobName(message.getJobName())
                            .consumerRef(message.getConsumerRef())
                            .timestamp(Instant.now().toString())
                            .eventType(message.getEventType())
                            .startTime(otStartTime)
                            .endTime(Instant.now())
                            .customerCount(finalCustomerCount)
                            .batchFiles(Collections.emptyList())
                            .success(false)
                            .retryFlag(retryFlag)
                            .retryCount(retryCount)
                            .errorCode("POST_PROCESSING_ERROR")
                            .errorMessage(ex.getMessage())
                            .build();

                    sendToAuditTopic(fmcompleteAuditFail);
                }
            });

        } catch (Exception ex) {
            logger.error("❌ [batchId: {}] Kafka message processing failed. Error: {}", batchId, ex.getMessage(), ex);
            ack.acknowledge();
        }
    }

==========
package com.nedbank.kafka.filemanage.service;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@ConfigurationProperties(prefix = "source")
@Data
public class SourceSystemProperties {

    private List<SystemConfig> systems;

    @Data
    public static class SystemConfig {
        private String name;      // e.g. NEDTRUST
        private String url;       // e.g. ${OT_SERVICE_CADNT1Service_URL}
        private String token;     // e.g. otds-token-dev
        private String jobName;   // optional - only for systems with multiple job names
    }

    /**
     * Find configuration for given source system and job name.
     * Falls back to generic (no jobName) config if specific not found.
     */
    public Optional<SystemConfig> getConfigForSourceSystem(String sourceSystem, String jobName) {

        // 1️⃣ Try to find exact match (sourceSystem + jobName)
        Optional<SystemConfig> match = systems.stream()
                .filter(s -> s.getName().equalsIgnoreCase(sourceSystem)
                        && s.getJobName() != null
                        && s.getJobName().equalsIgnoreCase(jobName))
                .findFirst();

        // 2️⃣ If not found, try generic (no jobName) match
        if (match.isEmpty()) {
            match = systems.stream()
                    .filter(s -> s.getName().equalsIgnoreCase(sourceSystem)
                            && s.getJobName() == null)
                    .findFirst();
        }

        // 3️⃣ Return final result
        return match;
    }
}
