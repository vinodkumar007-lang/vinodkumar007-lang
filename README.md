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

        kafkaConsumer.subscribe(Collections.singletonList(inputTopic));
    }

    public ApiResponse listen() {
        logger.info("Polling Kafka topic '{}'", inputTopic);
        List<SummaryPayload> summaryPayloads = new ArrayList<>();

        try {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                logger.info("No new messages in topic '{}'", inputTopic);
                return new ApiResponse("No new messages", "info", Collections.emptyList());
            }

            for (ConsumerRecord<String, String> record : records) {
                try {
                    logger.info("Processing message at offset {}", record.offset());
                    KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);
                    ApiResponse response = processSingleMessage(kafkaMessage);

                    String responseJson = objectMapper.writeValueAsString(response);
                    kafkaTemplate.send(outputTopic, responseJson);

                    if (response.getData() instanceof SummaryPayload) {
                        summaryPayloads.add((SummaryPayload) response.getData());
                    }

                    kafkaConsumer.commitSync(Collections.singletonMap(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    ));

                } catch (Exception ex) {
                    logger.error("Error processing message at offset {}", record.offset(), ex);
                    return new ApiResponse("Error processing message: " + ex.getMessage(), "error", null);
                }
            }

        } catch (Exception ex) {
            logger.error("Kafka polling failed", ex);
            return new ApiResponse("Polling error: " + ex.getMessage(), "error", null);
        }

        return new ApiResponse("Processed " + summaryPayloads.size() + " message(s)", "success", summaryPayloads);
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        if (message == null) return new ApiResponse("Empty message", "error", null);

        logger.info("Handling batchId={}, sourceSystem={}", message.getBatchId(), message.getSourceSystem());

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

        for (BatchFile file : message.getBatchFiles()) {
            try {
                String targetPath = buildTargetBlobPath(
                        message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                        message.getUniqueConsumerRef(), message.getJobName(), file.getFilename()
                );

                logger.info("Copying blob from '{}' to '{}'", file.getBlobUrl(), targetPath);
                String copiedUrl = blobStorageService.copyFileFromUrlToBlob(file.getBlobUrl(), targetPath);

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

        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(message.getBatchId());
        summary.setFileName("summary_" + message.getBatchId() + ".json");
        summary.setHeader(header);
        summary.setMetadata(metadata);
        summary.setPayload(payload);
        summary.setProcessedFiles(processedFiles);
        summary.setPrintFiles(printFiles);

        File localSummaryFile = new File(System.getProperty("java.io.tmpdir"), summary.getFileName());
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(localSummaryFile, summary);
            logger.info("Written summary.json locally at {}", localSummaryFile.getAbsolutePath());

            String summaryBlobPath = String.format("%s/summary/%s/%s",
                    message.getSourceSystem(), message.getBatchId(), summary.getFileName());

            summaryFileUrl = blobStorageService.uploadFile(localSummaryFile.getAbsolutePath(), summaryBlobPath);
            summary.setSummaryFileURL(summaryFileUrl);

        } catch (IOException ex) {
            logger.error("Failed to write or upload summary.json", ex);
        }

        return new ApiResponse("Batch processed successfully", "success", summary);
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
