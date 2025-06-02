public ApiResponse listen() {
    logger.info("Subscribing to Kafka topic '{}'", inputTopic);
    kafkaConsumer.subscribe(Collections.singletonList(inputTopic));

    logger.info("Polling Kafka topic '{}'", inputTopic);
    List<Map<String, Object>> summaryPayloads = new ArrayList<>();
    String globalSummaryFileURL = null;

    try {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        if (records.isEmpty()) {
            logger.info("No new messages in topic '{}'", inputTopic);
            return new ApiResponse("No new messages", "info", Collections.emptyList());
        }

        for (ConsumerRecord<String, String> record : records) {
            try {
                logger.info("Processing message at offset {}", record.offset());
                KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);
                ApiResponse response = processSingleMessage(kafkaMessage);

                String responseJson = objectMapper.writeValueAsString(response);
                kafkaTemplate.send(outputTopic, responseJson);

                if (response.getData() instanceof Map) {
                    Map<String, Object> data = (Map<String, Object>) response.getData();

                    // Capture summaryFileURL from the first message only
                    if (globalSummaryFileURL == null && data.containsKey("summaryFileURL")) {
                        globalSummaryFileURL = (String) data.get("summaryFileURL");
                    }

                    // Remove summaryFileURL from individual message payloads
                    data.remove("summaryFileURL");

                    summaryPayloads.add(data);
                }

                kafkaConsumer.commitSync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                ));

            } catch (Exception ex) {
                logger.error("Error processing message at offset {}", record.offset(), ex);
                return new ApiResponse("Error processing message: " + ex.getMessage(), "error", null);
            }
        }

    } catch (Exception ex) {
        logger.error("Kafka polling failed", ex);
        return new ApiResponse("Polling error: " + ex.getMessage(), "error", null);
    }

    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put("summaryFileURL", globalSummaryFileURL);
    responseMap.put("messages", summaryPayloads);

    return new ApiResponse("Processed " + summaryPayloads.size() + " message(s)", "success", responseMap);
}
