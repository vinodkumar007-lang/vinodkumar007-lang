public Map<String, Object> listen() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    try {
        long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

        List<TopicPartition> partitions = new ArrayList<>();
        consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        );
        consumer.assign(partitions);

        for (TopicPartition partition : partitions) {
            if (lastProcessedOffsets.containsKey(partition)) {
                consumer.seek(partition, lastProcessedOffsets.get(partition) + 1);
            } else {
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                        consumer.offsetsForTimes(Collections.singletonMap(partition, threeDaysAgo));
                OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
                if (offsetAndTimestamp != null) {
                    consumer.seek(partition, offsetAndTimestamp.offset());
                } else {
                    consumer.seekToBeginning(Collections.singletonList(partition));
                }
            }
        }

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
        if (records.isEmpty()) {
            return generateErrorResponse("204", "No new messages available in Kafka topic.");
        }

        // Process ALL records, not just first
        for (ConsumerRecord<String, String> record : records) {
            SummaryPayload summaryPayload = processSingleMessage(record.value());

            // Append each message's payload
            SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);

            // Update last processed offset for this partition
            lastProcessedOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                record.offset()
            );
        }

        // Ideally commit offsets after processing
        consumer.commitSync();

        // Return some response, e.g., from last record or summary
        return buildFinalResponse(/* you can decide what to return here */);

    } catch (Exception e) {
        logger.error("Error while consuming Kafka message", e);
        return generateErrorResponse("500", "Internal Server Error while processing Kafka message.");
    } finally {
        consumer.close();
    }
}
