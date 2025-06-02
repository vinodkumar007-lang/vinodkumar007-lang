public ApiResponse listen() {
    logger.info("Subscribing to Kafka topic '{}'", inputTopic);
    kafkaConsumer.subscribe(Collections.singletonList(inputTopic));

    logger.info("Polling Kafka topic '{}'", inputTopic);
    try {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        if (records.isEmpty()) {
            logger.info("No new messages in topic '{}'", inputTopic);
            return new ApiResponse("No new messages", "info", Collections.emptyMap());
        }

        for (ConsumerRecord<String, String> record : records) {
            try {
                logger.info("Processing single message at offset {}", record.offset());
                KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);

                // Process only one message per request
                ApiResponse response = processSingleMessage(kafkaMessage);

                // Send to Kafka output topic
                String responseJson = objectMapper.writeValueAsString(response);
                kafkaTemplate.send(outputTopic, responseJson);

                // Commit offset after successful processing
                kafkaConsumer.commitSync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                ));

                // Return response exactly as expected
                Map<String, Object> data = (Map<String, Object>) response.getData();
                Map<String, Object> summaryPayload = new HashMap<>();
                summaryPayload.put("batchID", data.get("batchID"));
                summaryPayload.put("header", data.get("header"));
                summaryPayload.put("metadata", data.get("metadata"));
                summaryPayload.put("payload", data.get("payload"));
                summaryPayload.put("summaryFileURL", data.get("summaryFileURL"));
                summaryPayload.put("timestamp", data.get("timestamp"));

                return new ApiResponse("Batch processed successfully", "success", summaryPayload);

            } catch (Exception ex) {
                logger.error("Error processing message at offset {}", record.offset(), ex);
                return new ApiResponse("Error processing message: " + ex.getMessage(), "error", null);
            }
        }

    } catch (Exception ex) {
        logger.error("Kafka polling failed", ex);
        return new ApiResponse("Polling error: " + ex.getMessage(), "error", null);
    }

    return new ApiResponse("Unexpected processing outcome", "error", null);
}
