public ApiResponse listen() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "your-kafka-bootstrap"); // replace with your config
    props.put("group.id", "your-consumer-group");           // use consistent group ID
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("security.protocol", "SSL");                  // if using SSL
    // Add SSL configs if needed (keystore, truststore, etc.)

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        TopicPartition partition = new TopicPartition(inputTopic, 0);
        consumer.assign(Collections.singletonList(partition));

        // Get last committed offset
        OffsetAndMetadata committed = consumer.committed(partition);
        long nextOffset = committed != null ? committed.offset() : 0;

        consumer.seek(partition, nextOffset);

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        if (records.isEmpty()) {
            logger.info("No new messages at offset {}", nextOffset);
            return new ApiResponse("No new messages", "info", null);
        }

        for (ConsumerRecord<String, String> record : records) {
            try {
                KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);

                ApiResponse response = processSingleMessage(kafkaMessage);

                kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));

                // Manually commit offset
                consumer.commitSync(Collections.singletonMap(
                        partition,
                        new OffsetAndMetadata(record.offset() + 1)
                ));

                return response; // One message per request

            } catch (Exception ex) {
                logger.error("Error processing Kafka message", ex);
                return new ApiResponse("Error processing message: " + ex.getMessage(), "error", null);
            }
        }
    } catch (Exception e) {
        logger.error("Kafka consumer failed", e);
        return new ApiResponse("Kafka error: " + e.getMessage(), "error", null);
    }

    return new ApiResponse("No messages processed", "info", null);
}
