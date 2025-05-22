package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
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

        Map<String, Object> response = new HashMap<>();

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> result = handleMessage(record.value());
                if (result != null) return result;
            }
        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages");
        } finally {
            consumer.close();
        }

        return response;
    }

    private Map<String, Object> handleMessage(String message) throws JsonProcessingException {
        JsonNode root;

        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            message = convertPojoToJson(message);
            try {
                root = objectMapper.readTree(message);
            } catch (Exception retryEx) {
                logger.error("Failed to parse corrected JSON", retryEx);
                return generateErrorResponse("400", "Invalid JSON format");
            }
        }

        String batchId = extractField(root, "consumerReference");
        JsonNode batchFilesNode = root.get("batchFiles");

        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            return generateErrorResponse("404", "No batch files found");
        }

        JsonNode firstFile = batchFilesNode.get(0);
        String filePath = firstFile.get("fileLocation").asText();
        String objectId = firstFile.get("ObjectId").asText();

        String sasUrl;
        try {
            sasUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
        } catch (Exception e) {
            return generateErrorResponse("453", "Error generating SAS URL");
        }

        List<CustomerSummary> customerSummaries = new ArrayList<>();
        String fileName = "";
        String jobName = "";

        for (JsonNode fileNode : batchFilesNode) {
            String objId = fileNode.get("ObjectId").asText();
            String location = fileNode.get("fileLocation").asText();
            String extension = getFileExtension(location).toLowerCase();
            String customerId = objId.split("_")[0];

            if (fileNode.has("fileName")) fileName = fileNode.get("fileName").asText();
            if (fileNode.has("jobName")) jobName = fileNode.get("jobName").asText();

            CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
            detail.setObjectId(objId);
            detail.setFileLocation(location);
            detail.setFileUrl("file://" + location);
            detail.setStatus(extension.equals(".ps") ? "failed" : "OK");
            detail.setEncrypted(isEncrypted(location, extension));
            detail.setType(determineType(location, extension));

            CustomerSummary customer = customerSummaries.stream()
                    .filter(c -> c.getCustomerId().equals(customerId))
                    .findFirst()
                    .orElseGet(() -> {
                        CustomerSummary c = new CustomerSummary();
                        c.setCustomerId(customerId);
                        c.setAccountNumber("");
                        c.setFiles(new ArrayList<>());
                        customerSummaries.add(c);
                        return c;
                    });

            customer.getFiles().add(detail);
        }

        // Build summary.json structure
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("batchID", batchId);
        summary.put("fileName", fileName);

        Map<String, Object> header = new LinkedHashMap<>();
        header.put("tenantCode", extractField(root, "tenantCode"));
        header.put("channelID", extractField(root, "channelId"));
        header.put("audienceID", extractField(root, "audienceId"));
        header.put("timestamp", new Date().toInstant().toString());
        header.put("sourceSystem", extractField(root, "sourceSystem"));
        header.put("product", extractField(root, "product"));
        header.put("jobName", jobName);
        summary.put("header", header);

        List<Map<String, Object>> processedFiles = new ArrayList<>();
        List<Map<String, Object>> printFiles = new ArrayList<>();

        for (CustomerSummary customer : customerSummaries) {
            Map<String, Object> pf = new LinkedHashMap<>();
            pf.put("customerID", customer.getCustomerId());
            pf.put("accountNumber", customer.getAccountNumber());

            for (CustomerSummary.FileDetail detail : customer.getFiles()) {
                String key = switch (detail.getType()) {
                    case "pdf_archive" -> "pdfArchiveFileURL";
                    case "pdf_email" -> "pdfEmailFileURL";
                    case "html_email" -> "htmlEmailFileURL";
                    case "txt_email" -> "txtEmailFileURL";
                    case "pdf_mobstat" -> "pdfMobstatFileURL";
                    default -> null;
                };
                if (key != null) pf.put(key, detail.getFileUrl());

                if ("ps_print".equals(detail.getType())) {
                    Map<String, Object> print = new LinkedHashMap<>();
                    print.put("printFileURL", detail.getFileUrl());
                    printFiles.add(print);
                }
            }

            pf.put("statusCode", "OK");
            pf.put("statusDescription", "Success");
            processedFiles.add(pf);
        }

        summary.put("processedFiles", processedFiles);
        summary.put("printFiles", printFiles);

        // Write summary.json
        String userHome = System.getProperty("user.home");
        File summaryFile = new File(userHome, "summary.json");
        String summaryPath = summaryFile.getAbsolutePath();

        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, summary);
        } catch (IOException e) {
            return generateErrorResponse("601", "Failed to write summary file");
        }

        // Send Kafka message
        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", fileName);
        kafkaMsg.put("jobName", jobName);
        kafkaMsg.put("batchId", batchId);
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("pdfFileURL", sasUrl);
        kafkaTemplate.send(outputTopic, batchId, objectMapper.writeValueAsString(kafkaMsg));

        // Construct Response
        Map<String, Object> response = new LinkedHashMap<>();
        Map<String, Object> inner = new LinkedHashMap<>();

        inner.put("batchID", batchId);
        inner.put("header", header);
        inner.put("metadata", null);  // fill if needed
        inner.put("payload", Map.of(
                "uniqueConsumerRef", extractField(root, "uniqueConsumerRef"),
                "uniqueECPBatchRef", extractField(root, "uniqueECPBatchRef"),
                "runPriority", extractField(root, "runPriority"),
                "eventID", extractField(root, "eventID"),
                "eventType", extractField(root, "eventType"),
                "restartKey", extractField(root, "restartKey")
        ));
        inner.put("summaryFileURL", summaryPath);
        inner.put("timestamp", new Date().toString());

        response.put("response", inner);
        return response;
    }

    private boolean isEncrypted(String location, String ext) {
        return ext.equals(".pdf") && location.toLowerCase().contains("email");
    }

    private String determineType(String location, String ext) {
        if (location.contains("mobstat")) return "pdf_mobstat";
        if (location.contains("archive")) return "pdf_archive";
        if (location.contains("email")) {
            if (ext.equals(".pdf")) return "pdf_email";
            if (ext.equals(".html")) return "html_email";
            if (ext.equals(".txt")) return "txt_email";
        }
        if (ext.equals(".ps")) return "ps_print";
        return "unknown";
    }

    private String getFileExtension(String location) {
        return location.substring(location.lastIndexOf("."));
    }

    private String extractField(JsonNode root, String fieldName) {
        JsonNode node = root.get(fieldName);
        return node != null ? node.asText() : null;
    }

    private Map<String, Object> generateErrorResponse(String code, String msg) {
        Map<String, Object> err = new HashMap<>();
        err.put("errorCode", code);
        err.put("errorMessage", msg);
        return err;
    }

    private String convertPojoToJson(String pojo) {
        return pojo; // stubbed in case of conversion logic needed
    }
}
