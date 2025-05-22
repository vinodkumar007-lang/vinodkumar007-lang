public Map<String, Object> processAllMessages() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    List<TopicPartition> partitions = new ArrayList<>();
    for (int i = 0; i < totalPartitions; i++) {
        partitions.add(new TopicPartition(inputTopic, i));
    }
    consumer.assign(partitions);
    
    // Move the offset to the latest or earliest based on your needs
    consumer.seekToEnd(partitions);  // Use seekToEnd() for latest messages or seekToBeginning() for all messages

    try {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10)); // Increased polling time
        if (records.isEmpty()) {
            return generateErrorResponse("204", "No content processed from Kafka");
        }

        for (ConsumerRecord<String, String> record : records) {
            Map<String, Object> result = handleMessage(record.value());
            if (result != null) {
                return result;
            }
        }
    } catch (Exception e) {
        logger.error("Error during Kafka message processing", e);
        return generateErrorResponse("500", "Internal Server Error while processing messages");
    } finally {
        consumer.close();
    }

    return generateErrorResponse("204", "No content processed from Kafka");
}
