package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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

    public ApiResponse listen() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093");
        props.put("group.id", "str-ecp-batch");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
        props.put("ssl.truststore.password", "nedbank1");
        props.put("ssl.keystore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
        props.put("ssl.keystore.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.key.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put("ssl.protocol", "TLSv1.2");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(inputTopic, 0);
            consumer.assign(Collections.singletonList(partition));

            OffsetAndMetadata committed = consumer.committed(partition);
            long nextOffset = committed != null ? committed.offset() : 0;

            consumer.seek(partition, nextOffset);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                logger.info("No new messages at offset {}", nextOffset);
                // Return a non-null response with empty data objects
                return new ApiResponse(
                        "No new messages to process",
                        "info",
                        new SummaryPayloadResponse() // empty summary payload response to avoid nulls
                );
            }

            for (ConsumerRecord<String, String> record : records) {
                try {
                    KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);

                    ApiResponse response = processSingleMessage(kafkaMessage);

                    // Send response to output topic
                    kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));

                    // Commit offset after successful processing
                    consumer.commitSync(Collections.singletonMap(
                            partition,
                            new OffsetAndMetadata(record.offset() + 1)
                    ));

                    // Return the successful response
                    return response;

                } catch (Exception ex) {
                    logger.error("Error processing Kafka message", ex);
                    // Return error response with non-null fields
                    return new ApiResponse(
                            "Error processing message: " + ex.getMessage(),
                            "error",
                            new SummaryPayloadResponse() // empty object to avoid nulls
                    );
                }
            }
        } catch (Exception e) {
            logger.error("Kafka consumer failed", e);
            // Return error response with non-null fields
            return new ApiResponse(
                    "Kafka error: " + e.getMessage(),
                    "error",
                    new SummaryPayloadResponse() // empty object to avoid nulls
            );
        }

        // Should not reach here, but in case
        return new ApiResponse(
                "No messages processed",
                "info",
                new SummaryPayloadResponse() // empty object to avoid nulls
        );
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        if (message == null) {
            return new ApiResponse("Empty message", "error", new SummaryPayloadResponse());
        }

        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setTimestamp(instantToIsoString(message.getTimestamp()));
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());

        Payload payload = new Payload();
        payload.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payload.setRunPriority(message.getRunPriority());
        payload.setEventType(message.getEventType());

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

                // Handle full URL or blob name safely
                String resolvedBlobPath = extractBlobPath(sourceBlobUrl);

                // Download content from blob storage
                String sanitizedBlobName = extractFileName(resolvedBlobPath);
                String inputFileContent = blobStorageService.downloadFileContent(sanitizedBlobName);
                //String inputFileContent = blobStorageService.downloadFileContent(resolvedBlobPath);

                List<CustomerData> customers = DataParser.extractCustomerData(inputFileContent);

                for (CustomerData customer : customers) {
                    File pdfFile = FileGenerator.generatePdf(customer);
                    File htmlFile = FileGenerator.generateHtml(customer);
                    File txtFile = FileGenerator.generateTxt(customer);
                    File mobstatFile = FileGenerator.generateMobstat(customer);

                    String pdfArchiveBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "archive", customer.getAccountNumber(), pdfFile.getName());

                    String pdfEmailBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "email", customer.getAccountNumber(), pdfFile.getName());

                    String htmlEmailBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "html", customer.getAccountNumber(), htmlFile.getName());

                    String txtEmailBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "txt", customer.getAccountNumber(), txtFile.getName());

                    String mobstatBlobPath = buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                            message.getUniqueConsumerRef(), message.getJobName(), "mobstat", customer.getAccountNumber(), mobstatFile.getName());

                    String pdfArchiveUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(), pdfArchiveBlobPath);
                    String pdfEmailUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(), pdfEmailBlobPath);
                    String htmlEmailUrl = blobStorageService.uploadFile(htmlFile.getAbsolutePath(), htmlEmailBlobPath);
                    String txtEmailUrl = blobStorageService.uploadFile(txtFile.getAbsolutePath(), txtEmailBlobPath);
                    String mobstatUrl = blobStorageService.uploadFile(mobstatFile.getAbsolutePath(), mobstatBlobPath);

                    SummaryProcessedFile processedFile = new SummaryProcessedFile();
                    processedFile.setCustomerID(customer.getCustomerId());
                    processedFile.setAccountNumber(customer.getAccountNumber());
                    processedFile.setPdfArchiveFileURL(pdfArchiveUrl);
                    processedFile.setPdfEmailFileURL(pdfEmailUrl);
                    processedFile.setHtmlEmailFileURL(htmlEmailUrl);
                    processedFile.setTxtEmailFileURL(txtEmailUrl);
                    processedFile.setPdfMobstatFileURL(mobstatUrl);
                    processedFile.setStatusCode("OK");
                    processedFile.setStatusDescription("Success");

                    processedFiles.add(processedFile);
                    fileCount++;
                }
            } catch (Exception ex) {
                logger.warn("Error processing file '{}': {}", file.getFilename(), ex.getMessage());
            }
        }

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL(blobStorageService.buildPrintFileUrl(message));
        printFiles.add(printFile);

        metadata.setProcessedFileList(processedFiles);
        metadata.setPrintFile(printFiles);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName(fileName);
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);

        // Write summary JSON file and upload it
        String summaryJsonPath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload);
        summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message);
        summaryPayload.setSummaryFileURL(summaryFileUrl);

        // Build final response payload
        SummaryPayloadResponse apiPayload = new SummaryPayloadResponse();
        apiPayload.setMessage("Processed files: " + fileCount);
        apiPayload.setStatus("success");
        //apiPayload.setData(summaryPayload);
        apiPayload.setSummaryPayload(summaryPayload);

        return new ApiResponse(
                apiPayload.getMessage(),
                apiPayload.getStatus(),
                apiPayload // include full payload here for detailed data and summaryPayload fields
        );
    }

    private String buildBlobPath(String sourceSystem, long timestamp, String batchId,
                                 String uniqueConsumerRef, String jobName, String folder,
                                 String customerAccount, String fileName) {

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.systemDefault());
        String dateStr = dtf.format(Instant.ofEpochMilli(timestamp));

        return String.format("%s/%s/%s/%s/%s/%s/%s",
                sourceSystem, dateStr, batchId, uniqueConsumerRef, jobName, folder, fileName);
    }

    private String extractBlobPath(String fullUrl) {
        if (fullUrl == null) return "";
        try {
            URI uri = URI.create(fullUrl);
            String path = uri.getPath();
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            return path;
        } catch (Exception e) {
            return fullUrl;
        }
    }

    private String instantToIsoString(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).toString();
    }

    public String extractFileName(String fullPathOrUrl) {
        if (fullPathOrUrl == null || fullPathOrUrl.isEmpty()) {
            return fullPathOrUrl;
        }
        // Remove trailing slashes if any
        String trimmed = fullPathOrUrl.replaceAll("/+$", "");
        // Get the substring after last slash
        int lastSlashIndex = trimmed.lastIndexOf('/');
        if (lastSlashIndex >= 0 && lastSlashIndex < trimmed.length() - 1) {
            return trimmed.substring(lastSlashIndex + 1);
        } else {
            return trimmed; // no slashes found, return as is
        }
    }
}

package com.nedbank.kafka.filemanage.controller;

import com.nedbank.kafka.filemanage.model.ApiResponse;
import com.nedbank.kafka.filemanage.service.KafkaListenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/file")
public class FileProcessingController {

    private static final Logger logger = LoggerFactory.getLogger(FileProcessingController.class);
    private final KafkaListenerService kafkaListenerService;

    public FileProcessingController(KafkaListenerService kafkaListenerService) {
        this.kafkaListenerService = kafkaListenerService;
    }

    // Health check
    @GetMapping("/health")
    public String healthCheck() {
        logger.info("Health check endpoint hit.");
        return "File Processing Service is up and running.";
    }

    @PostMapping("/process")
    public ApiResponse triggerFileProcessing() {
        logger.info("POST /process called to trigger Kafka message processing.");
        return kafkaListenerService.listen();
    }
}
