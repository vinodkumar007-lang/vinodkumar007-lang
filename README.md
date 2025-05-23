public Map<String, Object> processAllMessages() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    try {
        // Get all partitions of the topic
        List<TopicPartition> partitions = consumer.partitionsFor(inputTopic).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();

        consumer.assign(partitions);
        consumer.poll(Duration.ofMillis(100)); // Ensure assignment
        consumer.seekToBeginning(partitions);  // Start from beginning

        List<String> allMessages = new ArrayList<>();
        int emptyPollCount = 0;

        while (emptyPollCount < 3) {  // Allow a few empty polls to ensure end of messages
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                emptyPollCount++;
            } else {
                emptyPollCount = 0; // Reset if we get data
                for (ConsumerRecord<String, String> record : records) {
                    allMessages.add(record.value());
                }
            }
        }

        if (allMessages.isEmpty()) {
            return generateErrorResponse("204", "No content processed from Kafka");
        }

        // Process all messages in bulk
        for (String msg : allMessages) {
            Map<String, Object> result = handleMessage(msg);
            // Optionally, collect or merge results here if needed
        }

        // Just return the last processed message's result or customize
        return Map.of("status", "success", "processedMessages", allMessages.size());

    } catch (Exception e) {
        logger.error("Error during Kafka message processing", e);
        return generateErrorResponse("500", "Internal Server Error while processing messages");
    } finally {
        consumer.close();
    }
}
