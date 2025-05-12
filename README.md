@KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
public void consumeKafkaMessage(ConsumerRecord<String, String> record) {
    logger.info("Kafka listener method entered.");
    String message = record.value();
    logger.info("Received Kafka message: {}", message);

    try {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(message);

        // Updated: Use "processReference" as batchId
        String batchId = root.path("processReference").asText();
        if (batchId == null || batchId.isEmpty()) {
            throw new IllegalArgumentException("Missing 'processReference' field in message.");
        }

        // Updated: Get fileLocation from first entry in batchFiles[]
        JsonNode batchFiles = root.path("batchFiles");
        if (!batchFiles.isArray() || batchFiles.isEmpty()) {
            throw new IllegalArgumentException("Missing or empty 'batchFiles' array in message.");
        }

        String filePath = batchFiles.get(0).path("fileLocation").asText();
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("Missing 'fileLocation' in first batchFiles entry.");
        }

        logger.info("Parsed batchId: {}, filePath: {}", batchId, filePath);

        // Upload to blob and send summary
        String blobUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId);
        logger.info("File uploaded to blob storage at URL: {}", blobUrl);

        Map<String, Object> summaryPayload = buildSummaryPayload(batchId, blobUrl);
        String summaryMessage = mapper.writeValueAsString(summaryPayload);

        kafkaTemplate.send(outputTopic, batchId, summaryMessage);
        logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

    } catch (Exception e) {
        logger.error("Error processing Kafka message: {}. Error: {}", message, e.getMessage(), e);
    }
}
