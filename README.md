ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
if (records.isEmpty()) {
    System.out.println("⚠️ No records found in this poll cycle.");
}
props.put("enable.auto.commit", "false");
consumer.seekToBeginning(consumer.assignment());
