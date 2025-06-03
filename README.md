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
                                BlobStorageService blobStorageService, SummaryJsonWriter summaryJsonWriter) {
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
                return new ApiResponse("No new messages", "info", null);
            }

            for (ConsumerRecord<String, String> record : records) {
                try {
                    KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);

                    ApiResponse response = processSingleMessage(kafkaMessage);

                    kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));

                    consumer.commitSync(Collections.singletonMap(
                            partition,
                            new OffsetAndMetadata(record.offset() + 1)
                    ));

                    return response;

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

                String inputFileContent = blobStorageService.downloadFileContent(sourceBlobUrl);

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
        printFile.setPrintFileURL(buildPrintFileUrl(message));
        printFiles.add(printFile);

        metadata.setProcessedFileList(processedFiles);
        metadata.setPrintFile(printFiles);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName(fileName);
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);
        summaryPayload.setPayload(payload);

        String summaryFilePath = SummaryJsonWriter.writeSummaryJson(summaryPayload);
        File summaryJsonFile = new File(summaryFilePath);

        try {
            String jsonContent = new String(java.nio.file.Files.readAllBytes(summaryJsonFile.toPath()));
            logger.info("📄 Summary JSON content before upload:\n{}", jsonContent);
        } catch (IOException e) {
            logger.warn("⚠️ Could not read summary.json for logging", e);
        }

        summaryFileUrl = blobStorageService.uploadFile(summaryFilePath, buildSummaryJsonBlobPath(message));
        summaryPayload.setSummaryFileURL(summaryFileUrl);
        payload.setFileCount(fileCount);

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

    private String buildBlobPath(String sourceSystem, Double timestamp, String batchId,
                                 String uniqueConsumerRef, String jobName, String folder,
                                 String customerAccountNumber, String fileName) {
        String datePart = instantToDateString(timestamp);
        return String.format("%s/%s/%s/%s/%s/%s/%s/%s",
                sourceSystem, datePart, batchId, uniqueConsumerRef, jobName, folder, customerAccountNumber, fileName);
    }

    private String buildSummaryJsonBlobPath(KafkaMessage message) {
        String datePart = Instant.ofEpochMilli(Long.parseLong(String.valueOf(message.getTimestamp())))
                .toString().substring(0, 10).replace("-", "");
        return String.format("%s/%s/%s/%s/%s/summary.json",
                message.getSourceSystem(), datePart,
                message.getBatchId(), message.getUniqueConsumerRef(), message.getJobName());
    }

    private String buildPrintFileUrl(KafkaMessage message) {
        String datePart = Instant.ofEpochMilli(Long.parseLong(String.valueOf(message.getTimestamp())))
                .toString().substring(0, 10).replace("-", "");
        String printFilePath = String.format("%s/%s/%s/%s/%s/print/%s_print.pdf",
                message.getSourceSystem(),
                datePart,
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                message.getJobName(),
                message.getBatchId());
        return String.format("https://%s.blob.core.windows.net/%s", azureBlobStorageAccount, printFilePath);
    }

    private String instantToIsoString(Double timestamp) {
        return Instant.ofEpochMilli(timestamp.longValue()).toString();
    }

    private String instantToDateString(Double timestamp) {
        return Instant.ofEpochMilli(timestamp.longValue())
                .atZone(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }
}
