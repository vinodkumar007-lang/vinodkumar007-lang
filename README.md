public Map<String, Object> processAllMessages() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();

    try {
        // Subscribe to topic (Kafka tracks offsets)
        consumer.subscribe(Collections.singletonList(inputTopic));
        logger.info("Subscribed to topic: {}", inputTopic);

        // Poll Kafka for new messages
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        logger.info("Polled {} records from topic '{}'", records.count(), inputTopic);

        for (ConsumerRecord<String, String> record : records) {
            logger.info("Received Kafka message: {}", record.value());

            Map<String, Object> result = handleMessage(record.value());
            if (result != null) return result;
        }

    } catch (Exception e) {
        logger.error("Error during Kafka message processing", e);
        return generateErrorResponse("500", "Internal Server Error while processing messages");

    } finally {
        consumer.close();
    }

    return generateErrorResponse("204", "No content processed from Kafka");
}
