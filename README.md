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
    public KafkaListenerService(
            KafkaTemplate<String, String> kafkaTemplate,
            BlobStorageService blobStorageService,
            KafkaConsumer<String, String> kafkaConsumer,
            @Value("${kafka.topic.input}") String inputTopic,
            @Value("${kafka.topic.output}") String outputTopic,
            @Value("${azure.blob.storage.account}") String azureBlobStorageAccount
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.kafkaConsumer = kafkaConsumer;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.azureBlobStorageAccount = azureBlobStorageAccount;
        this.kafkaConsumer.subscribe(Collections.singletonList(inputTopic));
    }

    public ApiResponse listen() {
        List<SummaryPayload> processedSummaries = new ArrayList<>();

        try {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));

            if (records.isEmpty()) {
                return new ApiResponse("No new messages found", "info", Collections.emptyList());
            }

            for (ConsumerRecord<String, String> record : records) {
                try {
                    KafkaMessage message = objectMapper.readValue(record.value(), KafkaMessage.class);
                    ApiResponse response = processSingleMessage(message);

                    String responseJson = objectMapper.writeValueAsString(response);
                    kafkaTemplate.send(outputTopic, responseJson);

                    if (response.getData() instanceof SummaryPayload) {
                        processedSummaries.add((SummaryPayload) response.getData());
                    }

                    kafkaConsumer.commitSync(Collections.singletonMap(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    ));
                } catch (Exception e) {
                    logger.error("Error processing Kafka message at offset {}", record.offset(), e);
                    return new ApiResponse("Failed to process message at offset " + record.offset(), "Error", null);
                }
            }

        } catch (Exception e) {
            logger.error("Kafka polling failed", e);
            return new ApiResponse("Kafka polling failed: " + e.getMessage(), "Error", null);
        }

        return new ApiResponse("Processed " + processedSummaries.size() + " message(s)", "Success", Collections.singletonList(processedSummaries));
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        if (message == null) {
            return new ApiResponse("No new Kafka messages found", "No Data", null);
        }

        List<SummaryProcessedFile> processedFiles = new ArrayList<>();
        List<PrintFile> printFiles = new ArrayList<>();

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

        Metadata metadata = new Metadata();
        int totalFilesProcessed = 0;

        String newBlobUrl = null;
        for (BatchFile batchFile : message.getBatchFiles()) {
            String targetBlobPath = buildTargetBlobPath(
                    message.getSourceSystem(),
                    message.getTimestamp(),
                    message.getBatchId(),
                    message.getUniqueConsumerRef(),
                    message.getJobName(),
                    batchFile.getFilename()
            );

            newBlobUrl = blobStorageService.copyFileFromUrlToBlob(batchFile.getBlobUrl(), targetBlobPath);

            SummaryProcessedFile spf = new SummaryProcessedFile();
            spf.setCustomerID(batchFile.getCustomerId()); // Replace with actual field
            spf.setAccountNumber(batchFile.getAccountNumber()); // Replace with actual field
            spf.setPdfArchiveFileURL(generatePdfUrl("archive", spf.getAccountNumber(), message.getBatchId()));
            spf.setPdfEmailFileURL(generatePdfUrl("email", spf.getAccountNumber(), message.getBatchId()));
            spf.setHtmlEmailFileURL(generatePdfUrl("html", spf.getAccountNumber(), message.getBatchId()));
            spf.setTxtEmailFileURL(generatePdfUrl("txt", spf.getAccountNumber(), message.getBatchId()));
            spf.setPdfMobstatFileURL(generatePdfUrl("mobstat", spf.getAccountNumber(), message.getBatchId()));
            spf.setStatusCode("OK");
            spf.setStatusDescription("Success");
            processedFiles.add(spf);

            totalFilesProcessed++;
        }

        PrintFile pf = new PrintFile();
        pf.setPrintFileURL("https://" + azureBlobStorageAccount + "/pdfs/mobstat/PrintFileName1_" + message.getBatchId() + ".ps");
        printFiles.add(pf);

        metadata.setTotalFilesProcessed(totalFilesProcessed);
        metadata.setProcessingStatus("Completed");
        metadata.setEventOutcomeCode("200");
        metadata.setEventOutcomeDescription("Batch processed successfully");

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName("summary_" + message.getBatchId() + ".json");
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);
        summaryPayload.setPayload(payload);
        summaryPayload.setFileLocation(newBlobUrl);
        summaryPayload.setProcessedFiles(processedFiles);
        summaryPayload.setPrintFiles(printFiles);

        File summaryFile = new File(System.getProperty("java.io.tmpdir"), "summary.json");
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, summaryPayload);
        } catch (IOException e) {
            logger.error("Failed to write summary.json", e);
            throw new RuntimeException(e);
        }

        String summaryBlobPath = String.format("%s/summary/%s/summary.json",
                message.getSourceSystem(), message.getBatchId());
        String summaryFileUrl = blobStorageService.uploadFile(String.valueOf(summaryFile), summaryBlobPath);
        summaryPayload.setSummaryFileURL(summaryFileUrl);

        return new ApiResponse("Batch processed successfully", "success", summaryPayload);
    }

    private String buildTargetBlobPath(String sourceSystem, Double timestamp, String batchId,
                                       String consumerRef, String processRef, String fileName) {
        String upperSourceSystem = sourceSystem != null ? sourceSystem.toUpperCase() : "UNKNOWN";
        return String.format("%s/input/%s", upperSourceSystem, fileName);
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
