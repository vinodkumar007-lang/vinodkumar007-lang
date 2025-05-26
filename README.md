public Map<String, Object> listen() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        try {
            long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
                    partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            );
            consumer.assign(partitions);

            for (TopicPartition partition : partitions) {
                if (lastProcessedOffsets.containsKey(partition)) {
                    consumer.seek(partition, lastProcessedOffsets.get(partition) + 1);
                } else {
                    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                            consumer.offsetsForTimes(Collections.singletonMap(partition, threeDaysAgo));
                    OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
                    if (offsetAndTimestamp != null) {
                        consumer.seek(partition, offsetAndTimestamp.offset());
                    } else {
                        consumer.seekToBeginning(Collections.singletonList(partition));
                    }
                }
            }

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            if (records.isEmpty()) {
                return generateErrorResponse("204", "No new messages available in Kafka topic.");
            }

            ConsumerRecord<String, String> firstRecord = records.iterator().next();
            SummaryPayload summaryPayload = processSingleMessage(firstRecord.value());

            // ⬇️ Write (append) the payload using your existing utility
            SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);

            // ⬇️ Track processed offset
            lastProcessedOffsets.put(
                    new TopicPartition(firstRecord.topic(), firstRecord.partition()),
                    firstRecord.offset()
            );

            return buildFinalResponse(summaryPayload);

        } catch (Exception e) {
            logger.error("Error while consuming Kafka message", e);
            return generateErrorResponse("500", "Internal Server Error while processing Kafka message.");
        } finally {
            consumer.close();
        }
    }
