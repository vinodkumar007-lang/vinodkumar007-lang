Instant startTime = Instant.now();
long customerCount = message.getBatchFiles().stream().mapToLong(BatchFile::getCustomerCount).sum();

ECPBatchAudit fmcomposeAudit = ECPBatchAudit.builder()
        .title("ECPBatchAudit")
        .type("object")
        .properties(ECPBatchAudit.Properties.builder()
                .datastreamName("Fmcompose")
                .datastreamType("logs")
                .batchId(message.getBatchId())
                .serviceName("Fmcompose")
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
                .startTime(startTime)
                .endTime(startTime)
                .customerCount(customerCount)
                .batchFiles(message.getBatchFiles().stream()
                        .map(f -> ECPBatchAudit.BatchFileAudit.builder()
                                .type("object")
                                .properties(ECPBatchAudit.BatchFileAudit.FileProperties.builder()
                                        .blobUrl(f.getBlobUrl())
                                        .fileName(f.getFileName())
                                        .fileType(f.getFileType())
                                        .build())
                                .build())
                        .collect(Collectors.toList()))
                .build())
        .success(true)
        .retryFlag(false)
        .retryCount(0)
        .build();

sendToAuditTopic(fmcomposeAudit);

=================================

Instant otEndTime = Instant.now();
String summaryUrl = summaryResponse.getSummaryFileURL();
String filename = summaryUrl.substring(summaryUrl.lastIndexOf('/') + 1);
String fileType = filename.contains(".") ? filename.substring(filename.lastIndexOf('.') + 1) : "";

ECPBatchAudit fmcompleteAudit = ECPBatchAudit.builder()
        .title("ECPBatchAudit")
        .type("object")
        .properties(ECPBatchAudit.Properties.builder()
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
                .batchFiles(Collections.singletonList(
                        ECPBatchAudit.BatchFileAudit.builder()
                                .type("object")
                                .properties(ECPBatchAudit.BatchFileAudit.FileProperties.builder()
                                        .blobUrl(summaryUrl)
                                        .fileName(filename)
                                        .fileType(fileType)
                                        .build())
                                .build()))
                .build())
        .success(otResponse.isSuccess())
        .errorCode(otResponse.isSuccess() ? null : "OT_FAILURE")
        .errorMessage(otResponse.isSuccess() ? null : otResponse.getMessage())
        .retryFlag(retryFlag)
        .retryCount(retryCount)
        .build();

sendToAuditTopic(fmcompleteAudit);

=============

ECPBatchAudit fmcompleteAuditFail = ECPBatchAudit.builder()
        .title("ECPBatchAudit")
        .type("object")
        .properties(ECPBatchAudit.Properties.builder()
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
                .build())
        .success(false)
        .errorCode("POST_PROCESSING_ERROR")
        .errorMessage(ex.getMessage())
        .retryFlag(retryFlag)
        .retryCount(retryCount)
        .build();

sendToAuditTopic(fmcompleteAuditFail);

