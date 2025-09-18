package com.nedbank.kafka.filemanage.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AuditTopicConsumerApp {

    public static void main(String[] args) {
        Properties props = new Properties();

        // 🔧 Kafka Consumer Config
        props.put("bootstrap.servers",
                "nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093");
        props.put("group.id", "audit-test-consumer");  // ⚠️ Use unique groupId for testing
        props.put("auto.offset.reset", "latest");      // Read new messages only
        props.put("enable.auto.commit", "true");

        // 🔐 SSL Config
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
        props.put("ssl.keystore.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.key.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.truststore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
        props.put("ssl.truststore.password", "nedbank1");
        props.put("ssl.protocol", "TLSv1.2");

        // 🔑 Deserialization
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();

        String auditTopic = "str-ecp-audit-topic"; // ✅ Replace with your actual audit topic name

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(auditTopic));
            System.out.println("✅ Subscribed to Audit Topic: " + auditTopic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    System.out.println("⏳ Waiting for audit messages...");
                }
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String prettyJson = writer.writeValueAsString(mapper.readTree(record.value()));
                        System.out.printf("📣 [Audit Event] Key=%s, Offset=%d:%n%s%n",
                                record.key(), record.offset(), prettyJson);
                    } catch (Exception e) {
                        System.err.println("⚠️ Invalid JSON in audit message: " + record.value());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
