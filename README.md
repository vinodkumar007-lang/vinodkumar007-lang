public Map<String, Object> processAllMessages() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    try {
        List<TopicPartition> partitions = consumer.partitionsFor(inputTopic).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();

        consumer.assign(partitions);
        consumer.poll(Duration.ofMillis(100));
        consumer.seekToBeginning(partitions);

        List<String> recentMessages = new ArrayList<>();
        int emptyPollCount = 0;

        long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

        while (emptyPollCount < 3) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                emptyPollCount++;
            } else {
                emptyPollCount = 0;
                for (ConsumerRecord<String, String> record : records) {
                    if (record.timestamp() >= threeDaysAgo) {
                        logger.info("✅ Received message from Kafka (offset={}): {}", record.offset(), record.value());
                        recentMessages.add(record.value());
                    } else {
                        logger.debug("⏩ Skipping old message (timestamp={}): {}", record.timestamp(), record.value());
                    }
                }
            }
        }

        if (recentMessages.isEmpty()) {
            return generateErrorResponse("204", "No content processed from Kafka");
        }

        SummaryPayload summaryPayload = processMessages(recentMessages);
        File jsonFile = writeSummaryToFile(summaryPayload);
        sendFinalResponseToKafka(summaryPayload, jsonFile);

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Batch processed successfully");
        response.put("status", "success");
        response.put("summaryPayload", summaryPayload);

        return response;

    } catch (Exception e) {
        logger.error("❌ Error during Kafka message processing", e);
        return generateErrorResponse("500", "Internal Server Error while processing messages");
    } finally {
        consumer.close();
    }
}
