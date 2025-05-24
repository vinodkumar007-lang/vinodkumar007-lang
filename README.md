public Map<String, Object> listen() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        try {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            );

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            // Poll once and get all records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            if (records.isEmpty()) {
                return generateErrorResponse("204", "No new messages found in Kafka topic.");
            }

            // Process only the first message in this batch
            ConsumerRecord<String, String> record = records.iterator().next();
            String message = record.value();

            logger.info("Processing Kafka message (offset={}): {}", record.offset(), message);

            SummaryPayload payload = processSingleMessage(message);

            // Update summary.json by merging
            SummaryJsonWriter.writeUpdatedSummaryJson(summaryFile, payload, azureBlobStorageAccount);

            // Send single message payload to output Kafka topic
            String payloadJson = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(outputTopic, payloadJson);

            // Build response map based on the single payload processed
            Map<String, Object> responseMap = buildFinalResponse(payload);
            return responseMap;

        } catch (Exception e) {
            logger.error("Error processing Kafka message", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages.");
        } finally {
            consumer.close();
        }
    }
