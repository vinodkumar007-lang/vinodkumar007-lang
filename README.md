package com.nedbank.kafka.filemanage.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaTestProducer {

    public static void main(String[] args) {
        String topic = "str-ecp-batch-composition";

        // TODO: Replace these with actual values or load from env/config
        String bootstrapServers = "your-kafka-broker:9093";
        String keystoreLocation = "/path/to/keystore.jks";
        String keystorePassword = "your-keystore-password";
        String keyPassword = "your-key-password";
        String truststoreLocation = "/path/to/truststore.jks";
        String truststorePassword = "your-truststore-password";
        String sslProtocol = "TLSv1.2";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", keystoreLocation);
        props.put("ssl.keystore.password", keystorePassword);
        props.put("ssl.key.password", keyPassword);
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.protocol", sslProtocol);

        // Your actual JSON message
        String testMessage = """
        {
          "BatchId" : "1c93525b-42d1-410a-9e26-aa957f19861a",
          "SourceSystem" : "DEBTMAN",
          "TenantCode" : "ZANBL",
          "ChannelID" : null,
          "AudienceID" : null,
          "Product" : "DEBTMAN",
          "JobName" : "DEBTMAN",
          "UniqueConsumerRef" : "6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f",
          "Timestamp" : 1748351245.695410900,
          "RunPriority" : null,
          "EventType" : null,
          "BatchFiles" : [ {
            "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
            "RepositoryId" : "BATCH",
            "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
            "Filename" : "DEBTMAN.csv",
            "ValidationStatus" : "valid"
          } ]
        }
        """;

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, testMessage);
            RecordMetadata metadata = producer.send(record).get(); // blocking send
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
