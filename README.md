{
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        try {
            long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(10).toMillis();
            List<TopicPartition> partitions = new ArrayList<>();

            // Step 1: Get topic partitions
            consumer.partitionsFor(inputTopic).forEach(partitionInfo -> {
                TopicPartition tp = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                partitions.add(tp);
            });
            consumer.assign(partitions);

            // Step 2: Fetch beginning and end offsets for debugging
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            for (TopicPartition partition : partitions) {
                logger.info("Partition {}: beginning offset = {}, end offset = {}",
                        partition.partition(),
                        beginningOffsets.get(partition),
                        endOffsets.get(partition)
                );

                if (lastProcessedOffsets.containsKey(partition)) {
                    long nextOffset = lastProcessedOffsets.get(partition) + 1;
                    logger.info("Seeking to next offset after last processed: {}", nextOffset);
                    consumer.seek(partition, nextOffset);
                } else {
                    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                            consumer.offsetsForTimes(Collections.singletonMap(partition, threeDaysAgo));
                    OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);

                    if (offsetAndTimestamp != null) {
                        logger.info("Offset for 3 days ago in partition {} = {}", partition.partition(), offsetAndTimestamp.offset());
                        consumer.seek(partition, offsetAndTimestamp.offset());
                    } else {
                        logger.warn("No offset found for 3 days ago in partition {}; seeking to beginning.", partition.partition());
                        consumer.seekToBeginning(Collections.singletonList(partition));
                    }
                }

                long currentPos = consumer.position(partition);
                logger.info("Current position after seek on partition {}: {}", partition.partition(), currentPos);
            }

            // Step 3: Poll for records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            logger.info("Polled {} record(s) from Kafka", records.count());

            if (records.isEmpty()) {
                return generateErrorResponse("204", "No new messages available in Kafka topic.");
            }

            // Step 4: Process first message only
            ConsumerRecord<String, String> firstRecord = records.iterator().next();
            SummaryPayload summaryPayload = processSingleMessage(firstRecord.value());

            // Step 5: Append to summary.json
            SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);

            // Step 6: Track offset
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
