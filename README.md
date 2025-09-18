========== AUDIT KAFKA PRODUCER CONFIG ==========
Bootstrap Servers : nbpigelpdev02.africa.nedcor.net:9093,nbpproelpdev01.africa.nedcor.net:9093,nbpinelpdev01.africa.nedcor.net:9093
Security Protocol : SSL
Truststore Location : /app/keystore/truststore.jks
Keystore Location   : nedbank1
Truststore Location : /app/keystore/truststore.jks
Keystore Location   : 3dX7y3Yz9Jv6L4F
Keystore Location   : 3dX7y3Yz9Jv6L4F
Retries             : TLSv1.2
=================================================

package com.nedbank.kafka.filemanage.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class StaticKafkaProducerTest {

    public static void main(String[] args) {
        // Change these values
        String topicName = "log-ecp-batch-audit";   // <-- Replace with your topic
        String bootstrapServers = "nbpigelpdev02.africa.nedcor.net:9093,nbpproelpdev01.africa.nedcor.net:9093,nbpinelpdev01.africa.nedcor.net:9093";

        Properties props = new Properties();

        // Basic Kafka configs
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // SSL configs
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
        props.put("ssl.truststore.password", "nedbank1");
        props.put("ssl.keystore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
        props.put("ssl.keystore.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.key.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.protocol", "TLSv1.2");
        props.put("ssl.endpoint.identification.algorithm", ""); // disable hostname verification if required

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String key = "testKey";
            String value = "Hello from static producer test at " + System.currentTimeMillis();

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(); // synchronous send for testing

            System.out.println("✅ Message sent successfully!");
            System.out.println("Topic: " + metadata.topic());
            System.out.println("Partition: " + metadata.partition());
            System.out.println("Offset: " + metadata.offset());

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("❌ Failed to send message to Kafka.");
        }
    }
}

