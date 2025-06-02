@Autowired
public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                            BlobStorageService blobStorageService,
                            KafkaConsumer<String, String> kafkaConsumer) {
    this.kafkaTemplate = kafkaTemplate;
    this.blobStorageService = blobStorageService;
    this.kafkaConsumer = kafkaConsumer;

    // Subscribe only once — at startup
    this.kafkaConsumer.subscribe(Collections.singletonList(inputTopic));
}

public ApiResponse listen() {
    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));

    if (records.isEmpty()) {
        return new ApiResponse("No new messages", "info", null);
    }

    ConsumerRecord<String, String> nextRecord = getNextRecord(records);
    if (nextRecord == null) {
        return new ApiResponse("No valid Kafka messages found", "info", null);
    }

    try {
        KafkaMessage kafkaMessage = objectMapper.readValue(nextRecord.value(), KafkaMessage.class);

        ApiResponse response = processSingleMessage(kafkaMessage);

        kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));

        kafkaConsumer.commitSync(Collections.singletonMap(
                new TopicPartition(nextRecord.topic(), nextRecord.partition()),
                new OffsetAndMetadata(nextRecord.offset() + 1)
        ));

        return response;

    } catch (Exception ex) {
        logger.error("Error processing message from Kafka", ex);
        return new ApiResponse("Error processing message: " + ex.getMessage(), "error", null);
    }
}
