public Map<String, Object> listen() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    try {
        long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();
        logger.info("Calculating Kafka seek time: 3 days ago = {}", new Date(threeDaysAgo));

        List<TopicPartition> partitions = new ArrayList<>();
        consumer.partitionsFor(inputTopic).forEach(partitionInfo -> {
            TopicPartition tp = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            partitions.add(tp);
            logger.info("Discovered partition: {}", tp);
        });

        consumer.assign(partitions);

        // Fetch earliest and latest offsets before seeking
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

        for (TopicPartition partition : partitions) {
            long beginningOffset = beginningOffsets.getOrDefault(partition, -1L);
            long endOffset = endOffsets.getOrDefault(partition, -1L);
            logger.info("Partition {} - Earliest Offset: {}, Latest Offset: {}", partition, beginningOffset, endOffset);

            if (lastProcessedOffsets.containsKey(partition)) {
                long offset = lastProcessedOffsets.get(partition) + 1;
                logger.info("Seeking to last processed offset + 1: {} for partition {}", offset, partition);
                consumer.seek(partition, offset);
            } else {
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                        consumer.offsetsForTimes(Collections.singletonMap(partition, threeDaysAgo));
                OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
                if (offsetAndTimestamp != null) {
                    logger.info("Seeking to offset {} for partition {} (timestamp = 3 days ago)", offsetAndTimestamp.offset(), partition);
                    consumer.seek(partition, offsetAndTimestamp.offset());
                } else {
                    logger.info("No offset found for 3 days ago. Seeking to beginning for partition {}", partition);
                    consumer.seekToBeginning(Collections.singletonList(partition));
                }
            }
        }

        logger.info("Polling Kafka for new records...");
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
        logger.info("Polled {} record(s) from Kafka", records.count());

        if (records.isEmpty()) {
            return generateErrorResponse("204", "No new messages available in Kafka topic.");
        }

        ConsumerRecord<String, String> firstRecord = records.iterator().next();
        logger.info("Processing first record: topic={}, partition={}, offset={}, key={}",
                firstRecord.topic(), firstRecord.partition(), firstRecord.offset(), firstRecord.key());

        SummaryPayload summaryPayload = processSingleMessage(firstRecord.value());

        SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);
        logger.info("✅ Successfully appended summary for offset {}", firstRecord.offset());

        lastProcessedOffsets.put(
                new TopicPartition(firstRecord.topic(), firstRecord.partition()),
                firstRecord.offset()
        );

        return buildFinalResponse(summaryPayload);

    } catch (Exception e) {
        logger.error("❌ Error while consuming Kafka message", e);
        return generateErrorResponse("500", "Internal Server Error while processing Kafka message.");
    } finally {
        consumer.close();
        logger.info("Kafka consumer closed");
    }
}
