package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.Duration;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${kafka.security.protocol}")
    private String securityProtocol;

    @Value("${kafka.ssl.truststore.location}")
    private String sslTruststoreLocation;

    @Value("${kafka.ssl.truststore.password}")
    private String sslTruststorePassword;

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

    /**
     * Create a new KafkaConsumer per request to avoid stale internal state.
     */
    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("security.protocol", securityProtocol);
        props.put("ssl.truststore.location", sslTruststoreLocation);
        props.put("ssl.truststore.password", sslTruststorePassword);

        return new KafkaConsumer<>(props);
    }

    /**
     * Polls Kafka and processes one message per call.
     * Commits offset manually after processing.
     */
    public ApiResponse listen() {
        try (KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer()) {
            kafkaConsumer.subscribe(Collections.singletonList(inputTopic));
            logger.info("Kafka consumer subscribed to topic: {}", inputTopic);

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));

            if (records.isEmpty()) {
                logger.info("No new messages available in Kafka at this time");
                return new ApiResponse("No new messages", "info", null);
            }

            for (ConsumerRecord<String, String> record : records) {
                try {
                    KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);

                    ApiResponse response = processSingleMessage(kafkaMessage);

                    // Send the response to output Kafka topic
                    kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));

                    // Commit offset manually after successful processing
                    kafkaConsumer.commitSync(Collections.singletonMap(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    ));

                    return response; // Process only one message per request

                } catch (Exception ex) {
                    logger.error("Error processing Kafka message", ex);
                    return new ApiResponse("Error processing message: " + ex.getMessage(), "error", null);
                }
            }
        } catch (Exception e) {
            logger.error("Kafka consumer error", e);
            return new ApiResponse("Kafka consumer error: " + e.getMessage(), "error", null);
        }

        // No valid messages processed fallback
        return new ApiResponse("No valid Kafka messages processed", "info", null);
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        if (message == null) {
            return new ApiResponse("Empty message", "error", null);
        }

        // Prepare header
        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setTimestamp(instantToIsoString(message.getTimestamp()));
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());

        // Prepare payload
        Payload payload = new Payload();
        payload.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payload.setRunPriority(message.getRunPriority());
        payload.setEventType(message.getEventType());

        // Process files
        List<SummaryProcessedFile> processedFiles = new ArrayList<>();
        List<PrintFile> printFiles = new ArrayList<>();
        Metadata metadata = new Metadata();
        String summaryFileUrl = null;
        int fileCount = 0;

        String fileName = message.getBatchId() + ".json";

        for (BatchFile file : message.getBatchFiles()) {
            try {
                String sourceBlobUrl = file.getBlobUrl();
                String inputFileName = file.getFilename();
                if (inputFileName != null && !inputFileName.isBlank()) {
                    fileName = inputFileName;
                }

                String blobPath = buildTargetBlobPath(
                        message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                        message.getUniqueConsumerRef(), message.getJobName(), inputFileName
                );

                String newBlobUrl = blobStorageService.copyFileFromUrlToBlob(sourceBlobUrl, blobPath);

                SummaryProcessedFile processedFile = new SummaryProcessedFile();
                processedFile.setCustomerID("C001"); // Example placeholder
                processedFile.setAccountNumber("123456789012345"); // Example placeholder
                processedFile.setPdfArchiveFileURL(generatePdfUrl("archive", "123456789012345", message.getBatchId()));
                processedFile.setPdfEmailFileURL(generatePdfUrl("email", "123456789012345", message.getBatchId()));
                processedFile.setHtmlEmailFileURL(generatePdfUrl("html", "123456789012345", message.getBatchId()));
                processedFile.setTxtEmailFileURL(generatePdfUrl("txt", "123456789012345", message.getBatchId()));
                processedFile.setPdfMobstatFileURL(generatePdfUrl("mobstat", "123456789012345", message.getBatchId()));
                processedFile.setStatusCode("OK");
                processedFile.setStatusDescription("Success");

                processedFiles.add(processedFile);
                fileCount++;

            } catch (Exception ex) {
                logger.warn("Error copying file '{}': {}", file.getFilename(), ex.getMessage());
            }
        }

        // Add print file example
        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL("https://" + azureBlobStorageAccount + "/pdfs/mobstat/PrintFile_" + message.getBatchId() + ".ps");
        printFiles.add(printFile);

        // Fill metadata
        metadata.setTotalFilesProcessed(fileCount);
        metadata.setProcessingStatus("Completed");
        metadata.setEventOutcomeCode("200");
        metadata.setEventOutcomeDescription("Batch processed successfully");

        // Create SummaryPayload
        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName(fileName);
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);
        summaryPayload.setPayload(payload);
        summaryPayload.setProcessedFiles(processedFiles);
        summaryPayload.setPrintFiles(printFiles);

        // Write summary JSON locally and upload to Azure Blob Storage
        File summaryJsonFile = new File(System.getProperty("java.io.tmpdir"), fileName);
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(summaryJsonFile, summaryPayload);

            String summaryBlobPath = String.format("%s/summary/%s/%s",
                    message.getSourceSystem(), message.getBatchId(), fileName);

            summaryFileUrl = blobStorageService.uploadFile(summaryJsonFile.getAbsolutePath(), summaryBlobPath);

        } catch (IOException e) {
            logger.error("Failed to write/upload summary.json", e);
        }

        // Prepare API response payload (excluding processedFiles and printFiles)
        SummaryPayload apiPayload = new SummaryPayload();
        apiPayload.setBatchID(summaryPayload.getBatchID());
        apiPayload.setFileName(summaryPayload.getFileName());
        apiPayload.setHeader(summaryPayload.getHeader());
        apiPayload.setMetadata(summaryPayload.getMetadata());
        apiPayload.setPayload(summaryPayload.getPayload());
        apiPayload.setSummaryFileURL(summaryFileUrl);
        apiPayload.setTimestamp(Instant.now().toString());

        return new ApiResponse("Batch processed successfully", "success", apiPayload);
    }

    private String buildTargetBlobPath(String sourceSystem, Double timestamp, String batchId,
                                       String consumerRef, String processRef, String fileName) {
        String ts = instantToIsoString(timestamp).replace(":", "-");
        return String.format("%s/input/%s/%s/%s_%s/%s",
                sourceSystem.toUpperCase(), ts, batchId, consumerRef, processRef, fileName);
    }

    private String instantToIsoString(Double timestamp) {
        return Instant.ofEpochSecond(timestamp.longValue()).toString();
    }

    private String generatePdfUrl(String type, String accountNumber, String batchId) {
        return String.format("https://%s/pdfs/%s/%s_%s.%s",
                azureBlobStorageAccount, type, accountNumber, batchId,
                type.equals("html") ? "html" : "pdf");
    }
}
