@KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}",
        containerFactory = "kafkaListenerContainerFactory")
public void consumeKafkaMessage(String message) {
    try {
        KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
        ApiResponse response = processSingleMessage(kafkaMessage);

        // ✅ Log final API response
        String responseJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response);
        logger.info("📦 Final API Response:\n{}", responseJson);

        // ✅ Kafka send with timeout (e.g., 30 seconds)
        kafkaTemplate.send(kafkaOutputTopic, responseJson).get(30, TimeUnit.SECONDS);
        logger.info("✅ Successfully sent response to Kafka topic {}", kafkaOutputTopic);
    } catch (Exception ex) {
        logger.error("❌ Error processing Kafka message or sending response", ex);
    }
}
// ✅ Log full summary.json content before uploading
try {
    String summaryJson = Files.readString(Paths.get(summaryPath));
    logger.info("📄 summary.json content before upload:\n{}", summaryJson);
} catch (Exception e) {
    logger.warn("⚠️ Failed to read summary.json before upload", e);
}
