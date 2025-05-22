public Map<String, Object> processAllMessages() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    try (consumer) {
        consumer.subscribe(Collections.singletonList(inputTopic));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        if (records.isEmpty()) {
            return generateErrorResponse("204", "No content processed from Kafka");
        }

        for (ConsumerRecord<String, String> record : records) {
            return handleMessage(record.value());
        }
    } catch (Exception e) {
        logger.error("Error during Kafka message processing", e);
        return generateErrorResponse("500", "Internal Server Error while processing messages");
    }
}
