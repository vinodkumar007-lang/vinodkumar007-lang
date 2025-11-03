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

            Optional<SourceSystemProperties.SystemConfig> configOpt =
                    sourceSystemProperties.getConfigForSourceSystem(
                            message.getSourceSystem(),
                            message.getJobName()
                    );

            String url;
            String token;
            if (configOpt.isPresent()) {
                SourceSystemProperties.SystemConfig config = configOpt.get();
                url = config.getUrl();
                String secretName = config.getToken();  // token here means secret name
                token = blobStorageService.getSecret(secretName); // fetch actual token
                logger.info("Using URL={} for {}:{} with secretName={} fetched token successfully",
                        url, message.getSourceSystem(), message.getJobName(), secretName);
            } else {
                token = "";
                url = null;
                logger.warn("No config found for sourceSystem={} and jobName={}",
                        message.getSourceSystem(), message.getJobName());
            }

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
