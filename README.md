package com.nedbank.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerMain {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", 
            "XETELPZKA01.africa.nedcor.net:9093," +
            "XETELPKA02.africa.nedcor.net:9093," +
            "XETELPKA03.africa.nedcor.net:9093");

        props.put("group.id", "ecp-batch-audit-consumer");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        // SSL config - hardcoded
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "/Users/your-user/test-truststore.jks");
        props.put("ssl.truststore.password", "truststorepass");
        props.put("ssl.keystore.location", "/Users/your-user/test-keystore.jks");
        props.put("ssl.keystore.password", "keystorepass");
        props.put("ssl.key.password", "keypass");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("log-ecp-batch-audit"));

        System.out.println("🟢 Kafka Consumer started. Listening to topic: log-ecp-batch-audit...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("📩 New Message Received:");
                    System.out.println("🔑 Key: " + record.key());
                    System.out.println("📝 Value: " + record.value());
                    System.out.println("📦 Partition: " + record.partition());
                    System.out.println("🧾 Offset: " + record.offset());
                    System.out.println("──────────────────────────────");
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Error while consuming: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
