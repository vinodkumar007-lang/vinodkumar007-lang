package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaConsumer<String, String> kafkaConsumer;

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.kafkaConsumer = kafkaConsumer;
    }

    public ApiResponse listen() {
        kafkaConsumer.subscribe(Collections.singletonList(inputTopic));
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));

        if (records.isEmpty()) {
            return new ApiResponse("No new messages", "info", null);
        }

        for (ConsumerRecord<String, String> record : records) {
            try {
                KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);

                ApiResponse response = processSingleMessage(kafkaMessage);

                kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));

                kafkaConsumer.commitSync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                ));

                return response;

            } catch (Exception ex) {
                logger.error("Error processing message", ex);
                return new ApiResponse("Error processing message: " + ex.getMessage(), "error", null);
            }
        }

        return new ApiResponse("No valid Kafka messages processed", "info", null);
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        if (message == null) return new ApiResponse("Empty message", "error", null);

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
        int fileCount = 0;
        String summaryFileUrl = null;
        String extractedFileName = message.getBatchId() + ".json";

        for (BatchFile file : message.getBatchFiles()) {
            try {
                String sourceBlobUrl = file.getBlobUrl();
                String fileName = file.getFilename();
                if (fileName != null && !fileName.isBlank()) {
                    extractedFileName = fileName;
                }

                String targetPath = buildTargetBlobPath(
                        message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                        message.getUniqueConsumerRef(), message.getJobName(), fileName
                );

                String copiedUrl = blobStorageService.copyFileFromUrlToBlob(sourceBlobUrl, targetPath);

                SummaryProcessedFile processedFile = new SummaryProcessedFile();
                processedFile.setCustomerID("C001");
                processedFile.setAccountNumber("123456789012345");
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
                logger.warn("Failed to process file '{}': {}", file.getFilename(), ex.getMessage());
            }
        }

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL("https://" + azureBlobStorageAccount + "/pdfs/mobstat/PrintFile_" + message.getBatchId() + ".ps");
        printFiles.add(printFile);

        metadata.setTotalFilesProcessed(fileCount);
        metadata.setProcessingStatus("Completed");
        metadata.setEventOutcomeCode("200");
        metadata.setEventOutcomeDescription("Batch processed successfully");

        SummaryPayload summaryForFile = new SummaryPayload();
        summaryForFile.setBatchID(message.getBatchId());
        summaryForFile.setFileName(extractedFileName);
        summaryForFile.setHeader(header);
        summaryForFile.setMetadata(metadata);
        summaryForFile.setPayload(payload);
        summaryForFile.setProcessedFiles(processedFiles);
        summaryForFile.setPrintFiles(printFiles);
        // summaryFileURL is not set here — not saved into summary.json

        File localSummaryFile = new File(System.getProperty("java.io.tmpdir"), extractedFileName);
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(localSummaryFile, summaryForFile);
            String summaryBlobPath = String.format("%s/summary/%s/%s",
                    message.getSourceSystem(), message.getBatchId(), extractedFileName);
            summaryFileUrl = blobStorageService.uploadFile(localSummaryFile.getAbsolutePath(), summaryBlobPath);
        } catch (IOException ex) {
            logger.error("Failed to write or upload summary.json", ex);
        }

        // For final response, build a clean object excluding printFiles & processedFiles
        SummaryPayload responsePayload = new SummaryPayload();
        responsePayload.setBatchID(summaryForFile.getBatchID());
        responsePayload.setFileName(summaryForFile.getFileName());
        responsePayload.setHeader(summaryForFile.getHeader());
        responsePayload.setMetadata(summaryForFile.getMetadata());
        responsePayload.setPayload(summaryForFile.getPayload());
        responsePayload.setSummaryFileURL(summaryFileUrl); // ONLY in API response
        responsePayload.setTimestamp(Instant.now().toString());

        return new ApiResponse("Batch processed successfully", "success", responsePayload);
    }

    private String buildTargetBlobPath(String sourceSystem, Double timestamp, String batchId,
                                       String consumerRef, String processRef, String fileName) {
        String ts = instantToIsoString(timestamp).replace(":", "-");
        return String.format("%s/input/%s/%s/%s_%s/%s",
                sourceSystem.toUpperCase(), ts, batchId, consumerRef, processRef, fileName);
    }

    private String instantToIsoString(Double timestamp) {
        long epochSeconds = timestamp.longValue();
        return Instant.ofEpochSecond(epochSeconds).toString();
    }

    private String generatePdfUrl(String type, String accountNumber, String batchId) {
        return String.format("https://%s/pdfs/%s/%s_%s.%s",
                azureBlobStorageAccount, type, accountNumber, batchId,
                type.equals("html") ? "html" : "pdf");
    }
}

package com.nedbank.kafka.filemanage.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.ssl.keystore.location}")
    private String keystoreLocation;

    @Value("${kafka.consumer.ssl.keystore.password}")
    private String keystorePassword;

    @Value("${kafka.consumer.ssl.key.password}")
    private String keyPassword;

    @Value("${kafka.consumer.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${kafka.consumer.ssl.truststore.password}")
    private String truststorePassword;

    @Value("${kafka.consumer.ssl.protocol}")
    private String sslProtocol;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", keystoreLocation);
        props.put("ssl.keystore.password", keystorePassword);
        props.put("ssl.key.password", keyPassword);
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.protocol", sslProtocol);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", keystoreLocation);
        props.put("ssl.keystore.password", keystorePassword);
        props.put("ssl.key.password", keyPassword);
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.protocol", sslProtocol);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // 🔥 Add manual KafkaConsumer bean for use in KafkaListenerService
    @Bean
    public KafkaConsumer<String, String> manualKafkaConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", keystoreLocation);
        props.put("ssl.keystore.password", keystorePassword);
        props.put("ssl.key.password", keyPassword);
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.protocol", sslProtocol);

        return new KafkaConsumer<>(props);
    }
}
