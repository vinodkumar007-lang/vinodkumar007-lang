import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SecureKafkaConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        // Kafka bootstrap servers
        props.put("bootstrap.servers", "nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093");

        // Consumer group
        props.put("group.id", "str-ecp-batch");

        // SSL Configuration
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
        props.put("ssl.keystore.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.key.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.truststore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
        props.put("ssl.truststore.password", "nedbank1");
        props.put("ssl.protocol", "TLSv1.2");

        // Deserializers
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Offset reset
        props.put("auto.offset.reset", "earliest");

        // Create consumer and subscribe
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("str-ecp-batch-composition"));

        System.out.println("✅ Listening for messages on topic: str-ecp-batch-composition...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("🟢 Received: key=%s, value=%s, offset=%d, partition=%d%n",
                            record.key(), record.value(), record.offset(), record.partition());
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Error during consumption: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("🔒 Kafka consumer closed.");
        }
    }
}
