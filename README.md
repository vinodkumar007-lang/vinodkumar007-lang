package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

    @KafkaListener(
            topics = "${kafka.topic.input}",
            groupId = "${kafka.consumer.group.id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeKafkaMessage(String message) {
        try {
            logger.info("Received Kafka message...");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);

            ApiResponse response = processSingleMessage(kafkaMessage);

            // Send to output Kafka topic
            kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));

            logger.info("Kafka message processed successfully. Response: {}", response.getMessage());
        } catch (Exception ex) {
            logger.error("Error processing Kafka message", ex);
            // Optional: DLQ handling can be added here
        }
    }

    private ApiResponse processSingleMessage(KafkaMessage message) throws UnsupportedEncodingException {
        if (message == null) {
            return new ApiResponse("Empty message", "error",
                    new SummaryPayloadResponse("Empty message", "error", new SummaryResponse()).getSummaryResponse());
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
        String summaryFileUrl;

        String fileName = null;
        if (message.getBatchFiles() != null && !message.getBatchFiles().isEmpty()) {
            String firstBlobUrl = message.getBatchFiles().get(0).getBlobUrl();
            String blobPath = extractBlobPath(firstBlobUrl);
            fileName = extractFileName(blobPath);
        }
        if (fileName == null || fileName.isEmpty()) {
            fileName = message.getBatchId() + "_summary.json";
        }

        for (BatchFile file : message.getBatchFiles()) {
            try {
                String inputFileContent = blobStorageService.downloadFileContent(extractFileName(extractBlobPath(file.getBlobUrl())));
                List<CustomerData> customers = DataParser.extractCustomerData(inputFileContent);
                if (customers.isEmpty()) continue;

                for (CustomerData customer : customers) {
                    File pdfFile = FileGenerator.generatePdf(customer);
                    File htmlFile = FileGenerator.generateHtml(customer);
                    File txtFile = FileGenerator.generateTxt(customer);
                    File mobstatFile = FileGenerator.generateMobstat(customer);

                    String pdfArchiveUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "archive",
                                    customer.getAccountNumber(), pdfFile.getName())).split("\\?")[0];

                    String pdfEmailUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "email",
                                    customer.getAccountNumber(), pdfFile.getName())).split("\\?")[0];

                    String htmlEmailUrl = blobStorageService.uploadFile(htmlFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "html",
                                    customer.getAccountNumber(), htmlFile.getName())).split("\\?")[0];

                    String txtEmailUrl = blobStorageService.uploadFile(txtFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "txt",
                                    customer.getAccountNumber(), txtFile.getName())).split("\\?")[0];

                    String mobstatUrl = blobStorageService.uploadFile(mobstatFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "mobstat",
                                    customer.getAccountNumber(), mobstatFile.getName())).split("\\?")[0];

                    SummaryProcessedFile processedFile = new SummaryProcessedFile();
                    processedFile.setCustomerId(customer.getCustomerId());
                    processedFile.setAccountNumber(customer.getAccountNumber());
                    processedFile.setPdfArchiveFileUrl(pdfArchiveUrl);
                    processedFile.setPdfEmailFileUrl(pdfEmailUrl);
                    processedFile.setHtmlEmailFileUrl(htmlEmailUrl);
                    processedFile.setTxtEmailFileUrl(txtEmailUrl);
                    processedFile.setPdfMobstatFileUrl(mobstatUrl);
                    processedFile.setStatusCode("OK");
                    processedFile.setStatusDescription("Success");
                    processedFiles.add(processedFile);
                }
            } catch (Exception ex) {
                logger.error("Error processing file '{}': {}", file.getFilename(), ex.getMessage(), ex);
            }
        }

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL(blobStorageService.buildPrintFileUrl(message));
        printFiles.add(printFile);

        payload.setFileCount(processedFiles.size());

        metadata.setProcessingStatus("Completed");
        metadata.setTotalFilesProcessed(processedFiles.size());
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName(fileName);
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);
        summaryPayload.setPayload(payload);
        summaryPayload.setProcessedFiles(processedFiles);
        summaryPayload.setPrintFiles(printFiles);

        // Write the summary JSON file
        String summaryJsonPath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload);

        String summaryFileName = "summary_" + message.getBatchId() + ".json";
        // Upload using the clean file name
        summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message, summaryFileName);
        String decodedUrl = URLDecoder.decode(summaryFileUrl, StandardCharsets.UTF_8);
        summaryPayload.setSummaryFileURL(decodedUrl);

        SummaryResponse summaryResponse = new SummaryResponse();
        summaryResponse.setBatchID(summaryPayload.getBatchID());
        summaryResponse.setFileName(summaryPayload.getFileName());
        summaryResponse.setHeader(summaryPayload.getHeader());
        summaryResponse.setMetadata(summaryPayload.getMetadata());
        summaryResponse.setPayload(summaryPayload.getPayload());
        summaryResponse.setSummaryFileURL(summaryPayload.getSummaryFileURL());
        summaryResponse.setTimestamp(String.valueOf(Instant.now()));

        SummaryPayloadResponse apiPayload = new SummaryPayloadResponse("Batch processed successfully", "success", summaryResponse);
        return new ApiResponse(apiPayload.getMessage(), apiPayload.getStatus(), apiPayload.getSummaryResponse());
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
            return path.startsWith("/") ? path.substring(1) : path;
        } catch (Exception e) {
            return fullUrl;
        }
    }

    public String extractFileName(String fullPathOrUrl) {
        if (fullPathOrUrl == null || fullPathOrUrl.isEmpty()) return fullPathOrUrl;
        String trimmed = fullPathOrUrl.replaceAll("/+", "/");
        int lastSlashIndex = trimmed.lastIndexOf('/');
        return lastSlashIndex >= 0 ? trimmed.substring(lastSlashIndex + 1) : trimmed;
    }

    private String instantToIsoString(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).toString();
    }
}

package com.nedbank.kafka.filemanage.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group.id}")
    private String consumerGroupId;

    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    @Value("${kafka.consumer.enable.auto.commit}")
    private String enableAutoCommit;

    @Value("${kafka.consumer.key.deserializer}")
    private String keyDeserializer;

    @Value("${kafka.consumer.value.deserializer}")
    private String valueDeserializer;

    @Value("${kafka.consumer.security.protocol}")
    private String securityProtocol;

    @Value("${kafka.consumer.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${kafka.consumer.ssl.truststore.password}")
    private String truststorePassword;

    @Value("${kafka.consumer.ssl.keystore.location}")
    private String keystoreLocation;

    @Value("${kafka.consumer.ssl.keystore.password}")
    private String keystorePassword;

    @Value("${kafka.consumer.ssl.key.password}")
    private String keyPassword;

    @Value("${kafka.consumer.ssl.protocol}")
    private String sslProtocol;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);
        props.put("ssl.endpoint.identification.algorithm", "");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setConcurrency(1);
        return factory;
    }
}
