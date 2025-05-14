@KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
public void consumeKafkaMessage(ConsumerRecord<String, String> record) {
    logger.info("Kafka listener method entered.");
    String message = record.value();
    logger.info("Received Kafka message: {}", message);

    Map<String, Object> summaryResponse = null;

    try {
        // Parse the incoming Kafka message
        JsonNode root = new ObjectMapper().readTree(message);

        // Extract necessary fields from the incoming Kafka message
        String batchId = extractField(root, "consumerReference");  // Using consumerReference as batchId
        JsonNode batchFilesNode = root.get("batchFiles");

        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            logger.warn("No batch files found in the message.");
            return;
        }

        // For now, take the first file entry for blob upload
        JsonNode firstFile = batchFilesNode.get(0);
        String filePath = firstFile.get("fileLocation").asText();
        String objectId = firstFile.get("ObjectId").asText();

        logger.info("Parsed batchId: {}, filePath: {}, objectId: {}", batchId, filePath, objectId);

        // Upload the file and generate the SAS URL
        String blobUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
        logger.info("File uploaded to blob storage at URL: {}", blobUrl);

        // Build the summary payload to send to the output Kafka topic
        summaryResponse = buildSummaryPayload(batchId, blobUrl, batchFilesNode);
        String summaryMessage = new ObjectMapper().writeValueAsString(summaryResponse);

        // Send the summary message to the Kafka output topic
        kafkaTemplate.send(outputTopic, batchId, summaryMessage);
        logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

    } catch (Exception e) {
        // Improved error handling with detailed logging
        logger.error("Error processing Kafka message: {}. Error: {}", message, e.getMessage(), e);
    }

    // Return the summary response after processing
    if (summaryResponse != null) {
        logger.info("Returning summary response: {}", summaryResponse);
    } else {
        logger.warn("Summary response is null.");
    }
}
