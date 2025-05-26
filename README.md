public Map<String, Object> listen() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    List<SummaryPayload> processedPayloads = new ArrayList<>();
    Map<TopicPartition, Long> newOffsets = new HashMap<>();

    try {
        List<TopicPartition> partitions = new ArrayList<>();

        // Discover and assign partitions
        consumer.partitionsFor(inputTopic).forEach(partitionInfo -> {
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        });
        consumer.assign(partitions);

        // Seek to next unprocessed offset or beginning
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

        // Poll messages
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

            // Add extra tracking info for offset management (optional, if you can extend SummaryPayload)
            summaryPayload.setTopic(record.topic());
            summaryPayload.setPartition(record.partition());
            summaryPayload.setOffset(record.offset());

            processedPayloads.add(summaryPayload);

            // Keep track of highest offset per partition for committing later
            newOffsets.put(currentPartition, record.offset());
        }

        if (processedPayloads.isEmpty()) {
            return generateErrorResponse("204", "No new valid messages available in Kafka topic.");
        }

        // Append all processed payloads at once to summary.json
        SummaryJsonWriter.appendToSummaryJson(summaryFile, processedPayloads, azureBlobStorageAccount);

        // Send to Kafka for all processed payloads before returning
        for (SummaryPayload payload : processedPayloads) {
            sendToKafka(payload);  // Assuming you have this method for sending payload
        }

        // Update last processed offsets only after successful append and sending
        lastProcessedOffsets.putAll(newOffsets);

        return buildBatchFinalResponse(processedPayloads);

    } catch (Exception e) {
        logger.error("Error while consuming Kafka message", e);
        return generateErrorResponse("500", "Internal Server Error while processing Kafka message.");
    } finally {
        consumer.close();
    }
}
