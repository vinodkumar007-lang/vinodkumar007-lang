package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import com.nedbank.kafka.filemanage.model.MetaDataInfo;
import com.nedbank.kafka.filemanage.model.PayloadInfo;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
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

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    private final File summaryFile = new File(System.getProperty("user.home"), "summary.json");

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public Map<String, Object> listen() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        try {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
                    partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            );

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            List<String> recentMessages = new ArrayList<>();
            int emptyPollCount = 0;
            long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

            while (emptyPollCount < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPollCount++;
                } else {
                    emptyPollCount = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.timestamp() >= threeDaysAgo) {
                            logger.info("Received message (offset={}): {}", record.offset(), record.value());
                            recentMessages.add(record.value());
                        } else {
                            logger.debug("Skipping old message (timestamp={}): {}", record.timestamp(), record.value());
                        }
                    }
                }
            }

            if (recentMessages.isEmpty()) {
                return generateErrorResponse("204", "No recent messages found in Kafka topic.");
            }

            List<SummaryPayload> processedPayloads = new ArrayList<>();
            for (String message : recentMessages) {
                SummaryPayload summaryPayload = processSingleMessage(message);
                appendSummaryToFile(summaryPayload);
                processedPayloads.add(summaryPayload);
            }

            SummaryPayload finalSummary = mergeSummaryPayloads(processedPayloads);

            String finalSummaryJson = objectMapper.writeValueAsString(finalSummary);
            kafkaTemplate.send(outputTopic, finalSummaryJson);
            logger.info("Final combined summary sent to topic: {}", outputTopic);

            // ✅ Custom response as per your required structure
            Map<String, Object> response = new HashMap<>();
            response.put("message", "Batch processed successfully");
            response.put("status", "success");

            Map<String, Object> summaryPayloadMap = new HashMap<>();

            summaryPayloadMap.put("batchID", finalSummary.getHeader() != null ? finalSummary.getHeader().getBatchId() : null);

            Map<String, Object> headerMap = new HashMap<>();
            HeaderInfo header = finalSummary.getHeader();
            if (header != null) {
                headerMap.put("tenantCode", header.getTenantCode());
                headerMap.put("channelID", header.getChannelID());
                headerMap.put("audienceID", header.getAudienceID());
                headerMap.put("timestamp", header.getTimestamp());
                headerMap.put("sourceSystem", header.getSourceSystem());
                headerMap.put("product", header.getProduct());
                headerMap.put("jobName", header.getJobName());
            }
            summaryPayloadMap.put("header", headerMap);

            Map<String, Object> metadataMap = new HashMap<>();
            MetaDataInfo metadata = finalSummary.getMetadata();
            int totalFiles = 0;
            if (metadata != null && metadata.getCustomerSummaries() != null) {
                totalFiles = metadata.getCustomerSummaries().stream()
                        .mapToInt(cs -> cs.getFiles() != null ? cs.getFiles().size() : 0)
                        .sum();
            }
            metadataMap.put("totalFilesProcessed", totalFiles);
            metadataMap.put("processingStatus", metadata != null ? metadata.getProcessingStatus() : null);
            metadataMap.put("eventOutcomeCode", metadata != null ? metadata.getEventOutcomeCode() : null);
            metadataMap.put("eventOutcomeDescription", metadata != null ? metadata.getEventOutcomeDescription() : null);
            summaryPayloadMap.put("metadata", metadataMap);

            Map<String, Object> payloadMap = new HashMap<>();
            PayloadInfo payload = finalSummary.getPayload();
            if (payload != null) {
                payloadMap.put("uniqueConsumerRef", payload.getUniqueConsumerRef());
                payloadMap.put("uniqueECPBatchRef", payload.getUniqueECPBatchRef());
                payloadMap.put("filenetObjectID", payload.getFilenetObjectID());
                payloadMap.put("repositoryID", payload.getRepositoryID());
                payloadMap.put("runPriority", payload.getRunPriority());
                payloadMap.put("eventID", payload.getEventID());
                payloadMap.put("eventType", payload.getEventType());
                payloadMap.put("restartKey", payload.getRestartKey());
            } else {
                payloadMap.put("uniqueConsumerRef", null);
                payloadMap.put("uniqueECPBatchRef", null);
                payloadMap.put("filenetObjectID", null);
                payloadMap.put("repositoryID", null);
                payloadMap.put("runPriority", null);
                payloadMap.put("eventID", null);
                payloadMap.put("eventType", null);
                payloadMap.put("restartKey", null);
            }
            summaryPayloadMap.put("payload", payloadMap);

            summaryPayloadMap.put("processedFiles", finalSummary.getProcessedFiles());
            summaryPayloadMap.put("summaryFileURL", summaryFile.getAbsolutePath());
            summaryPayloadMap.put("timestamp", new Date().toString());

            response.put("summaryPayload", summaryPayloadMap);

            return response;

        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages.");
        } finally {
            consumer.close();
        }
    }
