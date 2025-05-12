package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate, BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

    /**
     * Kafka Listener that consumes messages from the input topic
     * @param record the Kafka message record
     */
    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void consumeKafkaMessage(ConsumerRecord<String, String> record) {
        String message = record.value();
        logger.info("Received Kafka message: {}", message);

        try {
            // Extract necessary fields from the incoming Kafka message
            String batchId = extractField(message, "ecpBatchGuid");  // Extracting ecpBatchGuid as batchId
            String filePath = extractField(message, "blobInputId");

            logger.info("Parsed batchId: {}, filePath: {}", batchId, filePath);

            // Upload the file and generate the SAS URL
            String blobUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId);
            logger.info("File uploaded to blob storage at URL: {}", blobUrl);

            // Build and send the summary payload to the output Kafka topic
            Map<String, Object> summaryPayload = buildSummaryPayload(batchId, blobUrl);
            String summaryMessage = new ObjectMapper().writeValueAsString(summaryPayload);

            kafkaTemplate.send(outputTopic, batchId, summaryMessage);
            logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

        } catch (Exception e) {
            // Improved error handling with detailed logging
            logger.error("Error processing Kafka message: {}. Error: {}", message, e.getMessage(), e);
        }
    }

    /**
     * Extracts a field from the Kafka message
     * @param json the raw Kafka message in JSON format
     * @param fieldName the field to extract from the JSON
     * @return the value of the field
     */
    private String extractField(String json, String fieldName) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readTree(json).get(fieldName).asText();
        } catch (Exception e) {
            // Detailed logging for missing or incorrect fields
            logger.error("Failed to extract field '{}'. Error: {}", fieldName, e.getMessage(), e);
            throw new RuntimeException("Failed to extract " + fieldName + " from message: " + json, e);
        }
    }

    /**
     * Builds the summary payload to send to the output Kafka topic
     * @param batchId the batch ID
     * @param blobUrl the URL of the uploaded file in blob storage
     * @return a map containing the summary payload
     */
    private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl) {
        // Creating the processed files list
        List<ProcessedFileInfo> processedFiles = List.of(
                new ProcessedFileInfo("C001", blobUrl + "/pdfs/C001_" + batchId + ".pdf"),
                new ProcessedFileInfo("C002", blobUrl + "/pdfs/C002_" + batchId + ".pdf")
        );

        // Constructing the summary payload
        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(batchId);
        summary.setHeader(new HeaderInfo()); // Populate header if required
        summary.setMetadata(new MetadataInfo()); // Populate metadata if required
        summary.setPayload(new PayloadInfo()); // Populate payload if required
        summary.setProcessedFiles(processedFiles);
        summary.setSummaryFileURL(blobUrl + "/summary/" + batchId + "_summary.json");

        // Convert the summary to a Map for easy sending to Kafka
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(summary, Map.class);
    }
}
package com.nedbank.kafka.filemanage.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.ssl.keystore.location}")
    private String keystoreLocation;

    @Value("${kafka.consumer.ssl.keystore.password}")
    private String keystorePassword;

    @Value("${kafka.consumer.ssl.key.password}")
    private String keyPassword;

    @Value("${kafka.consumer.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${kafka.consumer.ssl.truststore.password}")
    private String truststorePassword;

    @Value("${kafka.consumer.ssl.protocol}")
    private String sslProtocol;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", keystoreLocation);
        props.put("ssl.keystore.password", keystorePassword);
        props.put("ssl.key.password", keyPassword);
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.protocol", sslProtocol);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
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
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
