Consumer<String, String> consumer = consumerFactory.createConsumer();
try {
    List<TopicPartition> partitions = consumer.partitionsFor(inputTopic).stream()
            .map(info -> new TopicPartition(info.topic(), info.partition()))
            .toList();

    consumer.assign(partitions);
    consumer.poll(Duration.ofMillis(100));
    consumer.seekToEnd(partitions);  // <-- Changed here

    List<String> allMessages = new ArrayList<>();
    int emptyPollCount = 0;

    while (emptyPollCount < 3) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) {
            emptyPollCount++;
        } else {
            emptyPollCount = 0;
            for (ConsumerRecord<String, String> record : records) {
                allMessages.add(record.value());
            }
        }
    }

    if (allMessages.isEmpty()) {
        return generateErrorResponse("204", "No content processed from Kafka");
    }

    SummaryPayload summaryPayload = processMessages(allMessages);
    // ... rest of your code
