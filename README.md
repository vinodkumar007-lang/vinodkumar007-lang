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

package com.nedbank.kafka.filemanage.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaTestProducer {
    @Value("${kafka.bootstrap.servers}")
    private static String bootstrapServers;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.ssl.keystore.location}")
    private static String keystoreLocation;

    @Value("${kafka.consumer.ssl.keystore.password}")
    private static String keystorePassword;

    @Value("${kafka.consumer.ssl.key.password}")
    private static String keyPassword;

    @Value("${kafka.consumer.ssl.truststore.location}")
    private static String truststoreLocation;

    @Value("${kafka.consumer.ssl.truststore.password}")
    private static String truststorePassword;

    @Value("${kafka.consumer.ssl.protocol}")
    private static String sslProtocol;

    public static void main(String[] args) {
        String topic = "str-ecp-batch-composition";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", keystoreLocation);
        props.put("ssl.keystore.password", keystorePassword);
        props.put("ssl.key.password", keyPassword);
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.protocol", sslProtocol);
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

