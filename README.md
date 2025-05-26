public void processMessages(boolean singleMessageMode) {
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
        consumer.subscribe(Collections.singletonList(kafkaTopic));

        Map<TopicPartition, Long> partitionOffsets = getOffsetsFromDaysAgo(consumer, Duration.ofDays(10));
        for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
            consumer.seek(entry.getKey(), entry.getValue());
            log.info("Seeking partition {} to offset from 10 days ago: {}", entry.getKey().partition(), entry.getValue());
        }

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        log.info("Polled {} record(s) from Kafka", records.count());

        if (records.isEmpty()) {
            log.info("No records to process.");
            return;
        }

        // Filter out already processed records
        List<ConsumerRecord<String, String>> unprocessedRecords = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            if (isAlreadyProcessed(record)) {
                continue;
            }
            unprocessedRecords.add(record);
            if (singleMessageMode) break; // process only the first new record
        }

        for (ConsumerRecord<String, String> record : unprocessedRecords) {
            processKafkaRecord(record);
            updateOffset(record); // remember last processed
            if (singleMessageMode) break; // explicitly break again for safety
        }

    } catch (Exception e) {
        log.error("Error while consuming messages: {}", e.getMessage(), e);
    }
}
private boolean isAlreadyProcessed(ConsumerRecord<String, String> record) {
    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
    return lastProcessedOffsets.getOrDefault(tp, -1L) >= record.offset();
}

private void updateOffset(ConsumerRecord<String, String> record) {
    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
    lastProcessedOffsets.put(tp, record.offset());
    log.info("Updated lastProcessedOffsets: {}", lastProcessedOffsets);
}
