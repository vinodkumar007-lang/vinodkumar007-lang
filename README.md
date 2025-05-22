import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

public Map<String, Object> processAllMessages() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();

    // Fetch metadata to get the partitions for the topic
    List<TopicPartition> partitions = getPartitionsForTopic(inputTopic);

    consumer.assign(partitions);

    // Move the offset to the latest or earliest based on your needs
    consumer.seekToEnd(partitions);  // Use seekToEnd() for the latest messages or seekToBeginning() for all messages

    try {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10)); // Increased polling time
        if (records.isEmpty()) {
            return generateErrorResponse("204", "No content processed from Kafka");
        }

        for (ConsumerRecord<String, String> record : records) {
            Map<String, Object> result = handleMessage(record.value());
            if (result != null) {
                return result;
            }
        }
    } catch (Exception e) {
        logger.error("Error during Kafka message processing", e);
        return generateErrorResponse("500", "Internal Server Error while processing messages");
    } finally {
        consumer.close();
    }

    return generateErrorResponse("204", "No content processed from Kafka");
}

// Helper method to get partitions for a given topic
private List<TopicPartition> getPartitionsForTopic(String topic) {
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerFactory().getConfigurationProperties());
    List<TopicPartition> partitions = new ArrayList<>();
    try {
        // Fetch the partition information for the topic
        List<org.apache.kafka.common.PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null) {
            for (org.apache.kafka.common.PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
        }
    } catch (Exception e) {
        logger.error("Error fetching partitions for topic: " + topic, e);
    } finally {
        consumer.close();
    }

    return partitions;
}
