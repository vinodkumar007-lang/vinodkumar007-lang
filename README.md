package com.nedbank.kafka.filemanage.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaTestProducer {

    public static void main(String[] args) {
        String topic = "str-ecp-batch-composition";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<your-bootstrap-server>"); // e.g. "kafka-broker:9093"
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // SSL Config (update with real paths and passwords or read from application.properties)
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", "<path-to-keystore.jks>");
        props.put("ssl.keystore.password", "<keystore-password>");
        props.put("ssl.key.password", "<key-password>");
        props.put("ssl.truststore.location", "<path-to-truststore.jks>");
        props.put("ssl.truststore.password", "<truststore-password>");
        props.put("ssl.protocol", "TLSv1.2");

        // Create the Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Test JSON message
        String testMessage = """
        {
            "sourceSystem": "TEST_SYS",
            "consumerReference": "TEST12345",
            "processReference": "PROC98765",
            "timestamp": "2025-05-27T15:00:00Z",
            "blobURL": "https://your-original-blob-url/file1.pdf",
            "eventOutcomeCode": "SUCCESS",
            "eventOutcomeDescription": "Processed Successfully",
            "customerSummary": {
                "customerId": "CUST123",
                "status": "SUCCESS"
            },
            "printFileURL": "https://your-original-blob-url/printfile1.pdf"
        }
        """;

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, testMessage);
            RecordMetadata metadata = producer.send(record).get(); // blocking
            System.out.printf("✅ Message sent to topic '%s', partition %d, offset %d%n",
                metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            System.err.println("❌ Failed to send message");
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
