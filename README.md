package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import com.nedbank.kafka.filemanage.model.MetaDataInfo;
import com.nedbank.kafka.filemanage.model.PayloadInfo;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.kafka.clients.consumer.*;
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

    // New: Map to store last processed offsets
    private final Map<TopicPartition, Long> lastProcessedOffsets = new HashMap<>();

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
            long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
                    partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            );
            consumer.assign(partitions);

            // Seek to the last processed offset or offset based on timestamp
            for (TopicPartition partition : partitions) {
                if (lastProcessedOffsets.containsKey(partition)) {
                    consumer.seek(partition, lastProcessedOffsets.get(partition) + 1);
                } else {
                    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                            consumer.offsetsForTimes(Collections.singletonMap(partition, threeDaysAgo));
                    OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
                    if (offsetAndTimestamp != null) {
                        consumer.seek(partition, offsetAndTimestamp.offset());
                    } else {
                        consumer.seekToBeginning(Collections.singletonList(partition));
                    }
                }
            }

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));

            if (records.isEmpty()) {
                return generateErrorResponse("204", "No new messages available in Kafka topic.");
            }

            ConsumerRecord<String, String> firstRecord = records.iterator().next();
            SummaryPayload summaryPayload = processSingleMessage(firstRecord.value());
            SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);

            // Update offset tracking
            lastProcessedOffsets.put(new TopicPartition(firstRecord.topic(), firstRecord.partition()), firstRecord.offset());

            return buildFinalResponse(summaryPayload);

        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages.");
        } finally {
            consumer.close();
        }
    }

    // Existing methods remain unchanged: processSingleMessage, buildHeader, etc.
    // Keep all helper methods as they are

    // ... (keep all original methods without modification)

}
