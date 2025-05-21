package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ConsumerFactory<String, String> consumerFactory;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public Map<String, Object> processAllMessages() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        consumer.assign(Collections.singletonList(new TopicPartition(inputTopic, 0)));
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition(inputTopic, 0)));

        Map<String, Object> summaryResponse = new HashMap<>();
        List<Map<String, Object>> processedBatchSummaries = new ArrayList<>();

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing record with key: {}, value: {}", record.key(), record.value());
                Map<String, Object> recordSummary = handleMessage(record.value());
                if (recordSummary != null) {
                    processedBatchSummaries.add(recordSummary);
                }
            }

            if (!processedBatchSummaries.isEmpty()) {
                summaryResponse.put("processedBatchSummaries", processedBatchSummaries);
                logger.info("Processed {} messages from Kafka topic: {}", processedBatchSummaries.size(), inputTopic);
            }
        } catch (Exception e) {
            logger.error("Error polling or processing Kafka record: {}", e.getMessage(), e);
            summaryResponse.put("status", "500");
            summaryResponse.put("message", "Internal Server Error while processing messages");
        } finally {
            consumer.close();
        }

        return summaryResponse;
    }

    private Map<String, Object> handleMessage(String message) throws Exception {
        JsonNode root;

        logger.info("Message check   " + message);
        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            logger.warn("Failed to parse JSON, attempting to convert POJO-like message: {}", message);
            message = convertPojoToJson(message);
            try {
                root = objectMapper.readTree(message);
            } catch (Exception retryEx) {
                logger.error("Failed to parse corrected JSON: {}", retryEx.getMessage(), retryEx);
                return null;
            }
        }

        String batchId = extractField(root, "consumerReference");
        JsonNode batchFilesNode = root.get("batchFiles");

        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            logger.warn("No batch files found in the message.");
            return null;
        }

        JsonNode firstFile = batchFilesNode.get(0);
        String filePath = firstFile.get("fileLocation").asText();
        String objectId = firstFile.get("ObjectId").asText();

        logger.info("Parsed batchId: {}, filePath: {}, objectId: {}", batchId, filePath, objectId);

        // Call to BlobStorageService to upload file and generate SAS URL
        String sasUrl = null;
        try {
            sasUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
        } catch (Exception e) {
            logger.error("Error during file upload and SAS generation: {}", e.getMessage(), e);
            return generateErrorResponse("500", "Failed to upload file or generate SAS URL");
        }

        // Generate summary with new format
        Map<String, Object> summaryResponse = buildSummaryPayload(batchId, sasUrl, batchFilesNode);

        String summaryMessage = objectMapper.writeValueAsString(summaryResponse);
        assert batchId != null;
        kafkaTemplate.send(outputTopic, batchId, summaryMessage);
        logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

        // Write the summary data to a summary.json file in the user's home directory
        writeSummaryFile(summaryResponse);

        return summaryResponse;
    }

    private void writeSummaryFile(Map<String, Object> summary) {
        String userHome = System.getProperty("user.home");
        File jsonFile = new File(userHome, "summary.json");

        try {
            objectMapper.writeValue(jsonFile, summary);
            logger.info("✅ summary.json written to {}", jsonFile.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to write summary.json to accessible path", e);
            // Handle error and send an appropriate response
        }
    }

    private Map<String, Object> buildSummaryPayload(String batchId, String sasUrl, JsonNode batchFilesNode) {
        List<ProcessedFileInfo> processedFiles = new ArrayList<>();
        List<CustomerSummary> customerSummaries = new ArrayList<>();

        Set<String> archivedCustomers = new HashSet<>();
        Set<String> emailedCustomers = new HashSet<>();
        Set<String> mobstatCustomers = new HashSet<>();
        Set<String> printCustomers = new HashSet<>();

        String fileName = "";
        String jobName = "";

        for (JsonNode fileNode : batchFilesNode) {
            String objectId = fileNode.get("ObjectId").asText();
            String fileLocation = fileNode.get("fileLocation").asText();
            String extension = getFileExtension(fileLocation).toLowerCase();
            String customerId = objectId.split("_")[0];

            if (fileNode.has("fileName")) fileName = fileNode.get("fileName").asText();
            if (fileNode.has("jobName")) jobName = fileNode.get("jobName").asText();

            String dynamicFileUrl = "file://" + fileLocation;

            CustomerSummary.FileDetail fileDetail = new CustomerSummary.FileDetail();
            fileDetail.setObjectId(objectId);
            fileDetail.setFileUrl(dynamicFileUrl);
            fileDetail.setFileLocation(fileLocation);
            fileDetail.setStatus(extension.equals(".ps") ? "failed" : "OK");
            fileDetail.setEncrypted(isEncrypted(fileLocation, extension));
            fileDetail.setType(determineType(fileLocation, extension));

            if (fileLocation.contains("mobstat")) mobstatCustomers.add(customerId);
            if (fileLocation.contains("archive")) archivedCustomers.add(customerId);
            if (fileLocation.contains("email")) emailedCustomers.add(customerId);
            if (extension.equals(".ps")) printCustomers.add(customerId);

            processedFiles.add(new ProcessedFileInfo(objectId, dynamicFileUrl));

            CustomerSummary customer = customerSummaries.stream()
                    .filter(c -> c.getCustomerId().equals(customerId))
                    .findFirst()
                    .orElseGet(() -> {
                        CustomerSummary newCustomer = new CustomerSummary();
                        newCustomer.setCustomerId(customerId);
                        newCustomer.setAccountNumber("");
                        newCustomer.setFiles(new ArrayList<>());
                        customerSummaries.add(newCustomer);
                        return newCustomer;
                    });

            customer.getFiles().add(fileDetail);
        }

        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("totalCustomersProcessed", customerSummaries.size());
        totals.put("totalArchived", archivedCustomers.size());
        totals.put("totalEmailed", emailedCustomers.size());
        totals.put("totalMobstat", mobstatCustomers.size());
        totals.put("totalPrint", printCustomers.size());

        Map<String, Object> fullSummary = new LinkedHashMap<>();
        fullSummary.put("fileName", fileName);
        fullSummary.put("jobName", jobName);
        fullSummary.put("batchId", batchId);
        fullSummary.put("timestamp", new Date().toString());
        fullSummary.put("customers", customerSummaries);
        fullSummary.put("totals", totals);

        Map<String, Object> summaryResponse = new LinkedHashMap<>();
        summaryResponse.put("fileName", fileName);
        summaryResponse.put("jobName", jobName);
        summaryResponse.put("batchId", batchId);
        summaryResponse.put("timestamp", new Date().toString());
        summaryResponse.put("summaryFileURL", sasUrl);
        summaryResponse.put("customers", customerSummaries);
        summaryResponse.put("totals", totals);

        return summaryResponse;
    }

    private boolean isEncrypted(String fileLocation, String extension) {
        return (extension.equals(".pdf") || extension.equals(".html") || extension.equals(".txt")) &&
                (fileLocation.toLowerCase().contains("mobstat") || fileLocation.toLowerCase().contains("email"));
    }

    private String determineType(String fileLocation, String extension) {
        if (fileLocation.contains("mobstat")) return "pdf_mobstat";
        if (fileLocation.contains("archive")) return "pdf_archive";
        if (fileLocation.contains("email")) {
            if (extension.equals(".html")) return "html_email";
            if (extension.equals(".txt")) return "txt_email";
            return "pdf_email";
        }
        if (extension.equals(".ps")) return "ps_print";
        return "unknown";
    }

    private String getFileExtension(String fileLocation) {
        int lastDotIndex = fileLocation.lastIndexOf('.');
        return lastDotIndex > 0 ? fileLocation.substring(lastDotIndex) : "";
    }

    private String extractField(JsonNode json, String fieldName) {
        try {
            JsonNode fieldNode = json.get(fieldName);
            return fieldNode != null ? fieldNode.asText() : null;
        } catch (Exception e) {
            logger.error("Failed to extract field '{}': {}", fieldName, e.getMessage(), e);
            return null;
        }
    }

    private String convertPojoToJson(String raw) {
        raw = raw.trim();
        if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
            raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
        }
        raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
        raw = raw.replaceAll(":([a-zA-Z0-9_]+)", ":\"$1\"");

        return "{" + raw + "}";
    }

    private Map<String, Object> generateErrorResponse(String errorCode, String errorMessage) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("status", errorCode);
        errorResponse.put("message", errorMessage);
        return errorResponse;
    }
}
