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

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void consumeKafkaMessage(ConsumerRecord<String, String> record) {
        String message = record.value();
        logger.info("Received Kafka message: {}", message);

        try {
            String batchId = extractField(message, "batchId");
            String filePath = extractField(message, "filePath");

            logger.info("Parsed batchId: {}, filePath: {}", batchId, filePath);

            String blobUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId);
            logger.info("File uploaded to blob storage at URL: {}", blobUrl);

            Map<String, Object> summaryPayload = buildSummaryPayload(batchId, blobUrl);
            String summaryMessage = new ObjectMapper().writeValueAsString(summaryPayload);

            kafkaTemplate.send(outputTopic, batchId, summaryMessage);
            logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

        } catch (Exception e) {
            logger.error("Error processing Kafka message: {}", e.getMessage(), e);
        }
    }

    private String extractField(String json, String fieldName) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readTree(json).get(fieldName).asText();
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract " + fieldName + " from message: " + json, e);
        }
    }

    private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl) {
        HeaderInfo header = new HeaderInfo();
        MetadataInfo metadata = new MetadataInfo();
        PayloadInfo payload = new PayloadInfo();

        List<ProcessedFileInfo> processedFiles = List.of(
                new ProcessedFileInfo("C001", blobUrl + "/pdfs/C001_" + batchId + ".pdf"),
                new ProcessedFileInfo("C002", blobUrl + "/pdfs/C002_" + batchId + ".pdf")
        );

        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(batchId);
        summary.setHeader(header);
        summary.setMetadata(metadata);
        summary.setPayload(payload);
        summary.setProcessedFiles(processedFiles);
        summary.setSummaryFileURL(blobUrl + "/summary/" + batchId + "_summary.json");

        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(summary, Map.class);
    }

    public void consumeMessageAndStoreFile(String message) {
        consumeKafkaMessage(new ConsumerRecord<>("manual", 0, 0, null, message));
    }
}
