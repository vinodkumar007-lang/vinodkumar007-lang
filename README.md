import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApp {

    public static void main(String[] args) {
        Properties props = new Properties();

        // Kafka Consumer Configuration
        props.put("bootstrap.servers", "nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093");
        props.put("group.id", "str-ecp-batch");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");

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

        // Jackson setup for pretty-printing JSON
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("str-ecp-batch-composition"));
            System.out.println("Listening to Kafka topic: str-ecp-batch-composition...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Parse and pretty-print JSON
                        String prettyJson = writer.writeValueAsString(mapper.readTree(record.value()));
                        System.out.printf("Consumed message [Key=%s, Partition=%d, Offset=%d]:\n%s\n",
                                record.key(), record.partition(), record.offset(), prettyJson);
                    } catch (Exception e) {
                        System.err.println("Invalid JSON message: " + record.value());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
