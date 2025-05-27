package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import com.nedbank.kafka.filemanage.model.ApiResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ConsumerFactory<String, String> consumerFactory;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public Map<String, Object> listen() {
        // Use try-with-resources to ensure consumer is closed
        try (KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer()) {
            consumer.subscribe(Collections.singletonList(inputTopic));
            logger.info("Subscribed to topic: {}", inputTopic);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            if (records.isEmpty()) {
                logger.info("No new Kafka messages found.");
                Map<String, Object> emptyResponse = new HashMap<>();
                emptyResponse.put("message", "No new messages found");
                emptyResponse.put("status", "empty");
                emptyResponse.put("summaryPayload", null);
                return emptyResponse;
            }

            ApiResponse lastResponse = null;

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing message at partition {}, offset {}", record.partition(), record.offset());

                try {
                    KafkaMessage message = objectMapper.readValue(record.value(), KafkaMessage.class);

                    // Process the message (your existing logic)
                    lastResponse = processSingleMessage(message);

                    // Send response to output Kafka topic
                    String responseJson = objectMapper.writeValueAsString(lastResponse);
                    kafkaTemplate.send(outputTopic, responseJson);
                    logger.info("Sent processed response to output topic: {}", outputTopic);

                } catch (Exception ex) {
                    logger.error("Error processing Kafka message at offset {}: {}", record.offset(), ex.getMessage(), ex);
                    // Optionally handle message failure or continue
                }
            }

            // Prepare final response for controller
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("message", lastResponse != null ? lastResponse.getMessage() : "No messages processed");
            responseMap.put("status", lastResponse != null ? lastResponse.getStatus() : "no_data");
            responseMap.put("summaryPayload", lastResponse != null ? lastResponse.getSummaryPayload() : null);
            return responseMap;

        } catch (Exception e) {
            logger.error("Exception during Kafka consumption", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("message", "Error processing messages: " + e.getMessage());
            errorResponse.put("status", "error");
            errorResponse.put("summaryPayload", null);
            return errorResponse;
        }
    }
