public Map<String, Object> processAllMessages() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    try {
        List<TopicPartition> partitions = consumer.partitionsFor(inputTopic).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();

        consumer.assign(partitions);

        // 👇 Go back to earliest to be able to filter based on timestamp
        consumer.seekToBeginning(partitions);

        // 👇 Define your time window (e.g., 3 days ago)
        long currentTimeMillis = System.currentTimeMillis();
        long threeDaysAgo = currentTimeMillis - Duration.ofDays(3).toMillis();

        List<String> recentMessages = new ArrayList<>();
        int emptyPollCount = 0;

        while (emptyPollCount < 3) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                emptyPollCount++;
            } else {
                emptyPollCount = 0;
                for (ConsumerRecord<String, String> record : records) {
                    if (record.timestamp() >= threeDaysAgo) {
                        recentMessages.add(record.value());
                    }
                }
            }
        }

        if (recentMessages.isEmpty()) {
            return generateErrorResponse("204", "No recent messages found in Kafka (last 3 days)");
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
        logger.error("Error during Kafka message processing", e);
        return generateErrorResponse("500", "Internal Server Error while processing messages");
    } finally {
        consumer.close();
    }
}
