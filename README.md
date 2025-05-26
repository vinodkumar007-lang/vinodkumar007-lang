public Map<String, Object> listen() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    try {
        long tenDaysAgo = System.currentTimeMillis() - Duration.ofDays(10).toMillis();
        List<TopicPartition> partitions = new ArrayList<>();

        // Step 1: Discover topic partitions
        consumer.partitionsFor(inputTopic).forEach(partitionInfo -> {
            TopicPartition tp = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            partitions.add(tp);
        });
        consumer.assign(partitions);

        // Step 2: Seek each partition individually
        for (TopicPartition partition : partitions) {
            if (lastProcessedOffsets.containsKey(partition)) {
                long nextOffset = lastProcessedOffsets.get(partition) + 1;
                consumer.seek(partition, nextOffset);
                logger.info("Seeking partition {} to offset {}", partition.partition(), nextOffset);
            } else {
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                        consumer.offsetsForTimes(Collections.singletonMap(partition, tenDaysAgo));
                OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
                if (offsetAndTimestamp != null) {
                    consumer.seek(partition, offsetAndTimestamp.offset());
                    logger.info("Seeking partition {} to offset from 10 days ago: {}", partition.partition(), offsetAndTimestamp.offset());
                } else {
                    consumer.seekToBeginning(Collections.singletonList(partition));
                    logger.warn("No timestamp offset found for partition {}; seeking to beginning", partition.partition());
                }
            }
        }

        // Step 3: Poll and process the first valid unprocessed message
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        logger.info("Polled {} record(s) from Kafka", records.count());

        for (ConsumerRecord<String, String> record : records) {
            TopicPartition currentPartition = new TopicPartition(record.topic(), record.partition());

            // Skip if already processed
            if (lastProcessedOffsets.containsKey(currentPartition) &&
                record.offset() <= lastProcessedOffsets.get(currentPartition)) {
                logger.debug("Skipping already processed offset {} for partition {}", record.offset(), record.partition());
                continue;
            }

            logger.info("Processing record from topic-partition-offset {}-{}-{}: key='{}'",
                    record.topic(), record.partition(), record.offset(), record.key());

            SummaryPayload summaryPayload = processSingleMessage(record.value());

            // Skip if invalid
            if (summaryPayload == null || summaryPayload.getBatchId() == null || summaryPayload.getBatchId().trim().isEmpty()) {
                logger.warn("Missing or empty mandatory field 'BatchId' at offset {}; skipping", record.offset());
                continue;
            }

            // Valid message → process and return
            SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);
            lastProcessedOffsets.put(currentPartition, record.offset());
            logger.info("Updated lastProcessedOffsets: {}", lastProcessedOffsets);

            return buildFinalResponse(summaryPayload); // Only one message per request
        }

        // If no valid unprocessed message found
        return generateErrorResponse("204", "No new valid messages available in Kafka topic.");

    } catch (Exception e) {
        logger.error("Error while consuming Kafka message", e);
        return generateErrorResponse("500", "Internal Server Error while processing Kafka message.");
    } finally {
        consumer.close();
    }
}
