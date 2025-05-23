List<TopicPartition> partitions = Collections.singletonList(new TopicPartition(inputTopic, 0));
consumer.assign(partitions);
consumer.poll(Duration.ofMillis(100)); // 🔧 Needed to ensure assignment happens
consumer.seekToBeginning(partitions);  // 🔧 Now seeking works properly
