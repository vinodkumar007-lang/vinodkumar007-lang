package com.nedbank.kafka.test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

public class KafkaProducerTestApp {

    public static void main(String[] args) {
        String topicName = "str-ecp-batch-composition-complete";
        String bootstrapServers = "nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // SSL Configuration
        props.put("security.protocol", "SSL");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "3dX7y3Yz9Jv6L4F");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "3dX7y3Yz9Jv6L4F");

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "nedbank1");

        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        props.put("ssl.endpoint.identification.algorithm", ""); // Disable hostname verification

        // Timeouts & retries
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "180000");
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "30000");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "test-key", "✅ Test message from Java producer");

            System.out.println("Sending message...");
            RecordMetadata metadata = producer.send(record).get(); // Synchronous to catch timeout

            System.out.printf("✅ Message sent successfully to topic=%s, partition=%d, offset=%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (Exception e) {
            System.err.println("❌ Failed to send message: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
