List<TopicPartition> partitions = Collections.singletonList(new TopicPartition(inputTopic, 0));
consumer.assign(partitions);
consumer.poll(Duration.ofMillis(100)); // Trigger assignment
consumer.seekToBeginning(partitions);  // Set position to beginning

// Now poll for actual data
ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
for (ConsumerRecord<String, String> record : records) {
    Map<String, Object> result = handleMessage(record.value());
    if (result != null) return result;
}
