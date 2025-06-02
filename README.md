package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
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

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

    // Subscribe once after bean creation
    /*@PostConstruct
    public void init() {
        kafkaConsumer.subscribe(Collections.singletonList(inputTopic));
        logger.info("Kafka consumer subscribed to topic: {}", inputTopic);
    }*/

    /**
     * Polls Kafka and processes one message per call.
     * Commits offset manually after processing.
     */
    public ApiResponse listen() {
        Properties props = new Properties();

// Kafka broker addresses
        props.put("bootstrap.servers", "nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093");

// Consumer group configuration
        props.put("group.id", "str-ecp-batch");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");

// Deserialization
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

// SSL configuration
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
        props.put("ssl.truststore.password", "nedbank1");
        props.put("ssl.keystore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
        props.put("ssl.keystore.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.key.password", "3dX7y3Yz9Jv6L4F");

// Optional: disable hostname verification if you're using self-signed certs or IPs
        props.put("ssl.endpoint.identification.algorithm", "");

// Optional: enforce specific protocol version if needed
        props.put("ssl.protocol", "TLSv1.2");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(inputTopic, 0);
            consumer.assign(Collections.singletonList(partition));

            // Get last committed offset
            OffsetAndMetadata committed = consumer.committed(partition);
            long nextOffset = committed != null ? committed.offset() : 0;

            consumer.seek(partition, nextOffset);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                logger.info("No new messages at offset {}", nextOffset);
                return new ApiResponse("No new messages", "info", null);
            }

            for (ConsumerRecord<String, String> record : records) {
                try {
                    KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);

                    ApiResponse response = processSingleMessage(kafkaMessage);

                    kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));

                    // Manually commit offset
                    consumer.commitSync(Collections.singletonMap(
                            partition,
                            new OffsetAndMetadata(record.offset() + 1)
                    ));

                    return response; // One message per request

                } catch (Exception ex) {
                    logger.error("Error processing Kafka message", ex);
                    return new ApiResponse("Error processing message: " + ex.getMessage(), "error", null);
                }
            }
        } catch (Exception e) {
            logger.error("Kafka consumer failed", e);
            return new ApiResponse("Kafka error: " + e.getMessage(), "error", null);
        }

        return new ApiResponse("No messages processed", "info", null);
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
        SummaryPayloadResponse apiPayload = new SummaryPayloadResponse();
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
