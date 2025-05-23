import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerLatest {

    public static void main(String[] args) {
        Properties props = new Properties();

        // Kafka Consumer Configuration
        props.put("bootstrap.servers", "nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093");
        props.put("group.id", "str-ecp-batch-latest");  // ⚠️ Use a new consumer group to avoid old committed offsets
        props.put("auto.offset.reset", "latest");       // 🟢 This is key
        props.put("enable.auto.commit", "true");

        // SSL Configuration
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
        props.put("ssl.keystore.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.key.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.truststore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
        props.put("ssl.truststore.password", "nedbank1");
        props.put("ssl.protocol", "TLSv1.2");

        // Deserialization
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("str-ecp-batch-composition"));
            System.out.println("Subscribed. Waiting for NEW messages...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    System.out.println("Waiting for new messages...");
                }
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String prettyJson = writer.writeValueAsString(mapper.readTree(record.value()));
                        System.out.printf("New message [Key=%s, Offset=%d]:\n%s\n",
                                record.key(), record.offset(), prettyJson);
                    } catch (Exception e) {
                        System.err.println("Invalid JSON: " + record.value());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
