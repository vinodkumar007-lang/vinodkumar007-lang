 public static SummaryPayload buildPayload(
            KafkaMessage kafkaMessage,
            List<SummaryProcessedFile> processedList,
            String summaryBlobUrl,
            String fileName,
            String batchId,
            String timestamp
    ) {
        SummaryPayload payload = new SummaryPayload();
        payload.setBatchID(batchId);
        payload.setFileName(fileName);
        payload.setTimestamp(timestamp);
        payload.setSummaryFileURL(summaryBlobUrl);

        // HEADER
        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        // METADATA
        Metadata metadata = new Metadata();
        metadata.setTotalFilesProcessed(processedList.size());
        metadata.setProcessingStatus("Completed");
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        payload.setMetadata(metadata);

        // PAYLOAD BLOCK
        Payload payloadInfo = new Payload();
        //payloadInfo.setUniqueConsumerRef(kafkaMessage.getConsumerReference());
        payloadInfo.setUniqueECPBatchRef(null);
        payloadInfo.setRunPriority(null);
        payloadInfo.setEventID(null);
        payloadInfo.setEventType(null);
        payloadInfo.setRestartKey(null);
        payloadInfo.setFileCount(processedList.size());
        payload.setPayload(payloadInfo);

        // PROCESSED FILES
        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
        //payload.setProcessedFiles(processedFileEntries);
        payload.setProcessedFileList(processedFileEntries);
        // TRIGGER FILE URL IF ANY
        payload.setMobstatTriggerFile(buildMobstatTrigger(processedList));

        return payload;
    }
