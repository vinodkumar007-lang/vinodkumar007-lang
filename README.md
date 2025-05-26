public Map<String, Object> listen() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    List<SummaryPayload> processedPayloads = new ArrayList<>();

    try {
        List<TopicPartition> partitions = new ArrayList<>();

        // Step 1: Discover and assign partitions
        consumer.partitionsFor(inputTopic).forEach(partitionInfo -> {
            TopicPartition tp = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            partitions.add(tp);
        });
        consumer.assign(partitions);

        // Step 2: Seek to next unprocessed offset or beginning
        for (TopicPartition partition : partitions) {
            if (lastProcessedOffsets.containsKey(partition)) {
                long nextOffset = lastProcessedOffsets.get(partition) + 1;
                consumer.seek(partition, nextOffset);
                logger.info("Seeking partition {} to offset {}", partition.partition(), nextOffset);
            } else {
                consumer.seekToBeginning(Collections.singletonList(partition));
                logger.warn("No previous offset for partition {}; seeking to beginning", partition.partition());
            }
        }

        // Step 3: Poll and process all valid unprocessed messages
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        logger.info("Polled {} record(s) from Kafka", records.count());

        for (ConsumerRecord<String, String> record : records) {
            TopicPartition currentPartition = new TopicPartition(record.topic(), record.partition());

            // Skip already processed messages
            if (lastProcessedOffsets.containsKey(currentPartition) &&
                record.offset() <= lastProcessedOffsets.get(currentPartition)) {
                logger.debug("Skipping already processed offset {} for partition {}", record.offset(), record.partition());
                continue;
            }

            logger.info("Processing record from topic-partition-offset {}-{}-{}: key='{}'",
                    record.topic(), record.partition(), record.offset(), record.key());

            SummaryPayload summaryPayload = processSingleMessage(record.value());

            if (summaryPayload == null || summaryPayload.getBatchId() == null || summaryPayload.getBatchId().trim().isEmpty()) {
                logger.warn("Missing or empty mandatory field 'BatchId' at offset {}; skipping", record.offset());
                continue;
            }

            // Valid payload → append to summary and update state
            SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);
            lastProcessedOffsets.put(currentPartition, record.offset());
            logger.info("Appended to summary.json and updated offset: {}", record.offset());

            processedPayloads.add(summaryPayload);
        }

        if (processedPayloads.isEmpty()) {
            return generateErrorResponse("204", "No new valid messages available in Kafka topic.");
        } else {
            return buildBatchFinalResponse(processedPayloads);
        }

    } catch (Exception e) {
        logger.error("Error while consuming Kafka message", e);
        return generateErrorResponse("500", "Internal Server Error while processing Kafka message.");
    } finally {
        consumer.close();
    }
}


private Map<String, Object> buildBatchFinalResponse(List<SummaryPayload> payloads) {
    List<Map<String, Object>> processedList = new ArrayList<>();

    for (SummaryPayload payload : payloads) {
        processedList.add(buildFinalResponse(payload));
    }

    Map<String, Object> batchResponse = new HashMap<>();
    batchResponse.put("statusCode", "200");
    batchResponse.put("message", "Batch processed successfully");
    batchResponse.put("messagesProcessed", processedList.size());
    batchResponse.put("payloads", processedList);

    return batchResponse;
}
