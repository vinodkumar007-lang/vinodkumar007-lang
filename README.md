public Map<String, Object> listen() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    try {
        long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

        List<TopicPartition> partitions = new ArrayList<>();
        consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        );
        consumer.assign(partitions);

        // Seek to offset based on timestamp (3 days ago)
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition partition : partitions) {
            timestampsToSearch.put(partition, threeDaysAgo);
        }

        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);
        for (TopicPartition partition : partitions) {
            OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
            if (offsetAndTimestamp != null) {
                consumer.seek(partition, offsetAndTimestamp.offset());
            } else {
                consumer.seekToBeginning(Collections.singletonList(partition));
            }
        }

        List<SummaryPayload> processedPayloads = new ArrayList<>();
        int emptyPollCount = 0;

        while (emptyPollCount < 3) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                emptyPollCount++;
            } else {
                emptyPollCount = 0;
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Processing message (offset={}): {}", record.offset(), record.value());
                    try {
                        SummaryPayload summaryPayload = processSingleMessage(record.value());
                        SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);
                        processedPayloads.add(summaryPayload);
                    } catch (Exception ex) {
                        logger.error("Failed to process message: {}", ex.getMessage(), ex);
                    }
                }
            }
        }

        if (processedPayloads.isEmpty()) {
            return generateErrorResponse("204", "No recent messages found in Kafka topic.");
        }

        SummaryPayload finalSummary = mergeSummaryPayloads(processedPayloads);
        String finalSummaryJson = objectMapper.writeValueAsString(finalSummary);

        kafkaTemplate.send(outputTopic, finalSummaryJson);
        logger.info("Final combined summary sent to topic: {}", outputTopic);

        return buildFinalResponse(finalSummary);

    } catch (Exception e) {
        logger.error("Error during Kafka message processing", e);
        return generateErrorResponse("500", "Internal Server Error while processing messages.");
    } finally {
        consumer.close();
    }
}
