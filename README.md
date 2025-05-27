public ApiResponse listen() {
        logger.info("Starting manual poll of Kafka messages from topic '{}'", inputTopic);

        List<SummaryPayload> processedSummaries = new ArrayList<>();

        try {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));

            if (records.isEmpty()) {
                logger.info("No new messages found in topic '{}'", inputTopic);
                return new ApiResponse("No new messages found", "Success", Collections.emptyList());
            }

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing message from topic {}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());

                try {
                    KafkaMessage message = objectMapper.readValue(record.value(), KafkaMessage.class);
                    ApiResponse response = processSingleMessage(message);

                    // Send response to producer topic
                    String responseJson = objectMapper.writeValueAsString(response);
                    kafkaTemplate.send(outputTopic, responseJson);

                    // Collect the summary payload
                    if (response.getSummaryPayload() != null) {
                        processedSummaries.add(response.getSummaryPayload());
                    }

                    // Commit this record's offset
                    kafkaConsumer.commitSync(Collections.singletonMap(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    ));

                } catch (Exception e) {
                    logger.error("Error processing Kafka message at offset " + record.offset(), e);
                    return new ApiResponse("Failed to process message at offset " + record.offset() + ": " + e.getMessage(), "Error", null);
                }
            }

        } catch (Exception e) {
            logger.error("Error during Kafka polling", e);
            return new ApiResponse("Kafka polling failed: " + e.getMessage(), "Error", null);
        }

        return new ApiResponse("Processed " + processedSummaries.size() + " message(s)", "Success", processedSummaries);
    }
