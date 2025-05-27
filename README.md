import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String inputTopic;
    private final String outputTopic;
    private final ObjectMapper objectMapper;
    private final SummaryJsonWriter summaryJsonWriter;

    public KafkaListenerService(KafkaConsumer<String, String> kafkaConsumer,
                                KafkaTemplate<String, String> kafkaTemplate,
                                String inputTopic,
                                String outputTopic,
                                ObjectMapper objectMapper,
                                SummaryJsonWriter summaryJsonWriter) {
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaTemplate = kafkaTemplate;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.objectMapper = objectMapper;
        this.summaryJsonWriter = summaryJsonWriter;
    }

    public ApiResponse listen() {
        logger.info("Starting manual poll of Kafka messages from topic '{}'", inputTopic);

        List<SummaryPayload> processedSummaries = new ArrayList<>();
        List<Long> offsetsProcessed = new ArrayList<>();
        List<String> messageUUIDs = new ArrayList<>();
        Map<String, Long> blobUploadTimes = new HashMap<>();
        String timestamp = Instant.now().toString();

        try {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));

            if (records.isEmpty()) {
                logger.info("No new messages found in topic '{}'", inputTopic);
                return new ApiResponse("No new messages found",
                        "Success",
                        Collections.emptyList(),
                        inputTopic,
                        Collections.emptyList(),
                        timestamp,
                        0,
                        Collections.emptyList(),
                        Collections.emptyMap());
            }

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing message from topic {}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());

                try {
                    KafkaMessage message = objectMapper.readValue(record.value(), KafkaMessage.class);
                    String messageUUID = UUID.randomUUID().toString();
                    logger.info("Generated UUID {} for message at offset {}", messageUUID, record.offset());

                    Instant startUpload = Instant.now();
                    ApiResponse response = processSingleMessage(message);
                    Instant endUpload = Instant.now();

                    long uploadDurationMs = Duration.between(startUpload, endUpload).toMillis();
                    logger.info("Blob upload took {} ms for message UUID {}", uploadDurationMs, messageUUID);

                    // Assuming response contains summaryPayload and you want to track upload times per file
                    // For demo, put upload time keyed by messageUUID (or file name if available)
                    blobUploadTimes.put(messageUUID, uploadDurationMs);

                    // Send the response as Kafka producer message
                    String responseJson = objectMapper.writeValueAsString(response);
                    kafkaTemplate.send(outputTopic, responseJson);
                    logger.info("Sent response message to topic '{}'", outputTopic);

                    if (response.getSummaryPayload() != null) {
                        processedSummaries.add(response.getSummaryPayload());
                        offsetsProcessed.add(record.offset());
                        messageUUIDs.add(messageUUID);
                    }

                    // Commit offset after processing
                    kafkaConsumer.commitSync(Collections.singletonMap(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    ));
                    logger.info("Committed offset {} for partition {}", record.offset() + 1, record.partition());

                } catch (Exception e) {
                    logger.error("Error processing Kafka message at offset " + record.offset(), e);
                    return new ApiResponse("Failed at offset " + record.offset() + ": " + e.getMessage(),
                            "Error",
                            null,
                            inputTopic,
                            offsetsProcessed,
                            timestamp,
                            processedSummaries.size(),
                            messageUUIDs,
                            blobUploadTimes);
                }
            }

        } catch (Exception e) {
            logger.error("Error during Kafka polling", e);
            return new ApiResponse("Kafka polling failed: " + e.getMessage(),
                    "Error",
                    null,
                    inputTopic,
                    offsetsProcessed,
                    timestamp,
                    processedSummaries.size(),
                    messageUUIDs,
                    blobUploadTimes);
        }

        logger.info("Processed {} message(s) with offsets {}", processedSummaries.size(), offsetsProcessed);

        return new ApiResponse("Processed " + processedSummaries.size() + " message(s)",
                "Success",
                processedSummaries,
                inputTopic,
                offsetsProcessed,
                timestamp,
                processedSummaries.size(),
                messageUUIDs,
                blobUploadTimes);
    }
