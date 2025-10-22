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
            long customerCount = 0;
            for(int i=0; i< message.getBatchFiles().size(); i++) {
                customerCount = message.getBatchFiles().get(i).getCustomerCount();
            }
            AuditMessage inboundAudit = buildAuditMessage(message, startTime, startTime,
                    "FmConsume", message.getEventType(), customerCount);
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
            long finalCustomerCount = customerCount;
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
                            "Fmcomplete", message.getEventType(), finalCustomerCount);
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
{
  "datastreamName": "ecp_batch_composition",
  "datastreamType": "logs",
  "batchId": "f6a1bdb5-2fb2-4f77-a7d3-7087758a2572",
  "serviceName": "ecmbatch-archive-listener",
  "systemEnv": "DEV",
  "sourceSystem": "DEBTMAN",
  "tenantCode": "ZANBL",
  "channelID": "1",
  "audienceID": "",
  "product": "DEBTMAN",
  "jobName": "DEBTMAN",
  "consumerRef": "",
  "timestamp": "2025-10-16T08:45:00Z",
  "eventType": "",
  "startTime": "2025-10-16T08:00:00Z",
  "endTime": "2025-10-16T08:30:00Z",
  "customerCount": 150,
  "batchFiles": [
    {
      "blobUrl": "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/DEBTMAN/076f2b3c-37bc-4bcb-ab6a-29041acfc0f…,
      "fileName": "8000013557201_LHSL05.pdf",
      "fileType": "PDF"
    },
    {
      "blobUrl": "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/DEBTMAN/076f2b3c-37bc-4bcb-ab6a-29041acfc0f…,
      "fileName": "8001453741101_LHDL05E.pdf",
      "fileType": "PDF"
    }
  ],
  "success": true,
  "errorCode": "",
  "errorMessage": "",
  "retryFlag": false,
  "retryCount": 0
}


FileManager (Filemanager composition service) to the audit topic
Data element					Value
title	 	 			ECPBatchAudit
type		 			object
properties:					
	datastreamName				Fmcompose
	datastreamType				logs
 	batchId				Get value from event received
 	serviceName				Fmcompose
	systemEnv				DEV/ETE/QA/PROD
	sourceSystem				Get value from event received
	tenantCode				Get value from event received
	channelId				Get value from event received
	audienceId				Get value from event received
	product 				Get value from event received
	jobName				Get value from event – links to composition application
	consumerRef				Get value from event received
	timestamp				
	eventType				
	startTime				
	endTime				
	customerCount				
	batchFiles:				
		type			
		Items:	type		
			Properties:		
				bobUrl	Where data file has been copied to on azure blob storage - Get value from event received
				fileName	Get value from event received
				fileType	Get value from event received
					
	success				True/false
	errorCode				
	errorMessage				
	retryFlag				
	retryCount				
					
					


FileManager (Filemanager service complete) to the audit topic
Data element					Value
title	 	 			ECPBatchAudit
type		 			object
Properties:					
	datastreamName				Fmcomplete
	datastreamType				logs
 	batchId				Get value from event received
 	serviceName				Fmcomplete
	systemEnv				DEV/ETE/QA/PROD
	sourceSystem				Get value from event received
	tenantCode				Get value from event received
	channelId				Get value from event received
	audienceId				Get value from event received
	product 				Get value from event received
	jobName				Get value from event received
	consumerRef				Get value from event received
	timestamp				
	eventType				
	startTime				
	endTime				
	customerCount				
	batchFiles:				
		type			
		Items:	type		
			Properties:		
				blobUrl	Where summary file has been copied to on azure blob storage 
				fileName	Summary report name
				fileType	
					
	success				True/false
	errorCode				
	errorMessage				
	retryFlag				
	retryCount				
					





