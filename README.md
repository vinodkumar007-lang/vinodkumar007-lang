public SummaryPayload processSingleMessage() {
    ConsumerRecord<String, String> record = getNextUnprocessedRecord();
    if (record == null) {
        logger.info("No unprocessed Kafka messages found.");
        return null;
    }

    try {
        logger.info("Processing Kafka message with offset {}", record.offset());
        String message = record.value();

        SummaryPayload payload = objectMapper.readValue(message, SummaryPayload.class);

        // Extract header fields
        String batchId = payload.getHeader().getBatchId();
        String tenantCode = payload.getHeader().getTenantCode();
        String channelID = payload.getHeader().getChannelID();
        String audienceID = payload.getHeader().getAudienceID();
        String jobName = payload.getHeader().getJobName();
        String sourceSystem = payload.getHeader().getSourceSystem();
        String consumerReference = payload.getHeader().getConsumerReference();
        String processReference = payload.getHeader().getProcessReference();
        String timestamp = payload.getHeader().getTimestamp();

        // Upload file to Azure Blob Storage and get URL
        String blobUrl = uploadFileAndReturnLocation(payload, sourceSystem, consumerReference, processReference, timestamp);
        payload.setBlobUrl(blobUrl);

        // Append/update summary.json
        File summaryFile = new File("summary.json");
        SummaryJsonWriter.appendToSummaryJson(summaryFile, List.of(payload), azureBlobStorageAccount);

        // **Send payload to Kafka BEFORE returning final response**
        sendToKafka(payload);

        // Mark record as processed so it won't be reprocessed
        markRecordAsProcessed(record.offset());

        // Return the enriched payload as final response
        return payload;

    } catch (Exception e) {
        logger.error("Error processing Kafka message at offset {}", record.offset(), e);
        return null;
    }
}
