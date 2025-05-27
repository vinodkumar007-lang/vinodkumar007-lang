@Autowired
    public KafkaListenerService(
            KafkaTemplate<String, String> kafkaTemplate,
            BlobStorageService blobStorageService,
            KafkaConsumer<String, String> kafkaConsumer,
            @Value("${kafka.topic.input}") String inputTopic,
            @Value("${kafka.topic.output}") String outputTopic,
            @Value("${azure.blob.storage.account}") String azureBlobStorageAccount
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.kafkaConsumer = kafkaConsumer;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.azureBlobStorageAccount = azureBlobStorageAccount;

        // ✅ Subscribe after topic is initialized
        if (inputTopic == null || inputTopic.trim().isEmpty()) {
            throw new IllegalArgumentException("Input topic is null or empty");
        }
        this.kafkaConsumer.subscribe(Collections.singletonList(inputTopic));
    }

    public void listen() {
        logger.info("Starting manual poll of Kafka messages from topic '{}'", inputTopic);

        try {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                logger.info("No new messages found in topic '{}'", inputTopic);
                return;
            }

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing message from topic {}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());

                try {
                    KafkaMessage message = objectMapper.readValue(record.value(), KafkaMessage.class);
                    ApiResponse response = processSingleMessage(message);

                    String responseJson = objectMapper.writeValueAsString(response);
                    kafkaTemplate.send(outputTopic, responseJson);
                    logger.info("Sent processed response to Kafka topic {}", outputTopic);

                    kafkaConsumer.commitSync(Collections.singletonMap(
                            new TopicPartition(record.topic(), record.partition()),
                            new org.apache.kafka.clients.consumer.OffsetAndMetadata(record.offset() + 1)
                    ));

                } catch (Exception e) {
                    logger.error("Error processing Kafka message at offset " + record.offset(), e);
                }
            }

        } catch (Exception e) {
            logger.error("Error during Kafka polling", e);
        }
    }
