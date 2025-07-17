package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

@Component
public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static String writeSummaryJsonToFile(SummaryPayload payload) {
        if (payload == null) {
            logger.error("SummaryPayload is null. Cannot write summary.json.");
            throw new IllegalArgumentException("SummaryPayload cannot be null");
        }

        try {
            String batchId = Optional.ofNullable(payload.getBatchID()).orElse("unknown");
            String fileName = "summary_" + batchId + ".json";

            Path tempDir = Files.createTempDirectory("summaryFiles");
            Path summaryFilePath = tempDir.resolve(fileName);

            File summaryFile = summaryFilePath.toFile();
            if (summaryFile.exists()) {
                Files.delete(summaryFilePath);
                logger.warn("Existing summary file deleted: {}", summaryFilePath);
            }

            objectMapper.writeValue(summaryFile, payload);
            logger.info("✅ Summary JSON written at: {}", summaryFilePath);

            return summaryFilePath.toAbsolutePath().toString();

        } catch (Exception e) {
            logger.error("❌ Failed to write summary.json", e);
            throw new RuntimeException("Failed to write summary JSON", e);
        }
    }

    public static SummaryPayload buildPayload(KafkaMessage message,
                                              List<SummaryProcessedFile> processedFiles,
                                              List<PrintFile> printFiles,
                                              String mobstatTriggerPath,
                                              int customersProcessed) {

        SummaryPayload payload = new SummaryPayload();

        payload.setBatchID(message.getBatchId());
        payload.setFileName(message.getBatchId() + ".csv");
        payload.setMobstatTriggerFile(mobstatTriggerPath);

        // Header block
        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());
        header.setTimestamp(Instant.now().toString());
        payload.setHeader(header);

        // Determine overall status
        String overallStatus = "Completed";
        if (processedFiles != null && !processedFiles.isEmpty()) {
            boolean allFailed = processedFiles.stream().allMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()));
            boolean anyFailed = processedFiles.stream().anyMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()) || "PARTIAL".equalsIgnoreCase(f.getStatusCode()));

            if (allFailed) overallStatus = "Failure";
            else if (anyFailed) overallStatus = "Partial";
        }

        // Metadata block
        Metadata metadata = new Metadata();
        metadata.setTotalFilesProcessed(customersProcessed);
        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        payload.setMetadata(metadata);

        // Payload block
        Payload payloadDetails = new Payload();
        payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
        payloadDetails.setRunPriority(message.getRunPriority());
        payloadDetails.setEventID(message.getEventID());
        payloadDetails.setEventType(message.getEventType());
        payloadDetails.setRestartKey(message.getRestartKey());
        payloadDetails.setFileCount(processedFiles != null ? processedFiles.size() : 0);
        payload.setPayload(payloadDetails);

        // Processed files
        payload.setProcessedFiles(processedFiles != null ? processedFiles : new ArrayList<>());

        // Print files (✅ Make sure included)
        payload.setPrintFiles(printFiles != null ? printFiles : new ArrayList<>());

        return payload;
    }
}

package com.nedbank.kafka.filemanage.service;

import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.*;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @Autowired private KafkaConsumer<String, String> kafkaConsumer;
    @Autowired private FileGenerator fileGenerator;
    @Autowired private BlobStorageService blobStorageService;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;

    public void pollAndProcess() {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) return;

        for (ConsumerRecord<String, String> record : records) {
            try {
                KafkaMessage kafkaMessage = KafkaUtils.deserializeKafkaMessage(record.value());
                logger.info("🔁 Processing message for batchID: {}", kafkaMessage.getBatchId());

                Path jobDir = Files.createTempDirectory("jobDir");
                String originalFilePath = blobStorageService.downloadFileContent(kafkaMessage.getBlobURL(), jobDir);

                Map<String, List<String>> groupedCustomers = DataParser.groupCustomersByAccountAndID(originalFilePath);
                logger.info("👥 Total customer groups found: {}", groupedCustomers.size());

                Map<String, Map<String, String>> errorMap = DataParser.parseErrorMap(originalFilePath);
                List<SummaryProcessedFile> allProcessedFiles = new ArrayList<>();
                List<PrintFile> allPrintFiles = new ArrayList<>();
                String mobstatTriggerPath = null;

                for (Map.Entry<String, List<String>> groupEntry : groupedCustomers.entrySet()) {
                    List<String> customerLines = groupEntry.getValue();
                    Map<String, String> errorDetails = errorMap.getOrDefault(groupEntry.getKey(), new HashMap<>());

                    // Generate all format files
                    Map<String, Path> outputFiles = fileGenerator.generateFilesForCustomerGroup(customerLines, jobDir);
                    for (Map.Entry<String, Path> entry : outputFiles.entrySet()) {
                        String folder = entry.getKey();
                        Path localFile = entry.getValue();

                        String uploadedURL = blobStorageService.uploadFileAndReturnLocation(
                                localFile, kafkaMessage.getBatchId(), folder, kafkaMessage.getTenantCode());

                        SummaryProcessedFile summaryFile = new SummaryProcessedFile();
                        summaryFile.setAccountNumber(customerLines.get(0).split("\\|")[1]);
                        summaryFile.setCustomerId(customerLines.get(0).split("\\|")[2]);
                        summaryFile.setFileType(folder.toUpperCase());
                        summaryFile.setStatusCode(errorDetails.isEmpty() ? "SUCCESS" : "FAILURE");
                        summaryFile.setOutputMethod(folder.toUpperCase());
                        summaryFile.setBlobURL(uploadedURL);
                        summaryFile.setErrorDetails(errorDetails);
                        allProcessedFiles.add(summaryFile);

                        if (folder.equals("mobstat")) {
                            mobstatTriggerPath = uploadedURL;
                        }

                        if (folder.equals("print")) {
                            allPrintFiles.add(new PrintFile(localFile.getFileName().toString(), uploadedURL));
                        }
                    }
                }

                // Build and write summary.json
                SummaryPayload summaryPayload = SummaryJsonWriter.buildPayload(
                        kafkaMessage, allProcessedFiles, allPrintFiles, mobstatTriggerPath, groupedCustomers.size());

                String summaryFilePath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload);
                String summaryURL = blobStorageService.uploadFileAndReturnLocation(
                        Paths.get(summaryFilePath), kafkaMessage.getBatchId(), "summary", kafkaMessage.getTenantCode());

                logger.info("📄 Final Summary Payload: \n{}", new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(summaryPayload));

                // Send response message to output Kafka topic
                kafkaTemplate.send("output-topic", new ObjectMapper().writeValueAsString(summaryPayload));
                logger.info("✅ Sent response to output-topic for batchID: {}", kafkaMessage.getBatchId());

                kafkaConsumer.commitSync();
                logger.info("☑️ Committed Kafka offset for batchID: {}", kafkaMessage.getBatchId());

            } catch (Exception e) {
                logger.error("❌ Failed to process record", e);
            }
        }
    }
}
