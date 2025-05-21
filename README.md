package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.ProcessedFileInfo;
import com.nedbank.kafka.filemanage.model.SummaryFileInfo;
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
                if (result != null) {
                    return result;
                }
            }
        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            response.put("status", "500");
            response.put("message", "Internal Server Error while processing messages");
        } finally {
            consumer.close();
        }

        return response;
    }

    private Map<String, Object> handleMessage(String message) {
        JsonNode root;

        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            message = convertPojoToJson(message);
            try {
                root = objectMapper.readTree(message);
            } catch (Exception retryEx) {
                logger.error("Failed to parse corrected JSON: {}", retryEx.getMessage(), retryEx);
                return generateErrorResponse("400", "Invalid JSON format");
            }
        }

        String batchId = extractField(root, "consumerReference");
        JsonNode batchFilesNode = root.get("batchFiles");

        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            logger.warn("No batch files found in message.");
            return generateErrorResponse("404", "No batch files found");
        }

        JsonNode firstFile = batchFilesNode.get(0);
        String filePath = firstFile.get("fileLocation").asText();
        String objectId = firstFile.get("ObjectId").asText();

        String sasUrl;
        try {
            sasUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
        } catch (Exception e) {
            logger.error("Blob upload/SAS URL generation failed", e);
            return generateErrorResponse("453", "Error generating SAS URL");
        }

        List<CustomerSummary> customerSummaries = new ArrayList<>();
        String fileName = "";
        String jobName = "";

        Set<String> archived = new HashSet<>();
        Set<String> emailed = new HashSet<>();
        Set<String> mobstat = new HashSet<>();
        Set<String> printed = new HashSet<>();

        for (JsonNode fileNode : batchFilesNode) {
            String objId = fileNode.get("ObjectId").asText();
            String location = fileNode.get("fileLocation").asText();
            String extension = getFileExtension(location).toLowerCase();
            String customerId = objId.split("_")[0];

            if (fileNode.has("fileName")) fileName = fileNode.get("fileName").asText();
            if (fileNode.has("jobName")) jobName = fileNode.get("jobName").asText();

            String fileUrl = "file://" + location;

            CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
            detail.setObjectId(objId);
            detail.setFileUrl(fileUrl);
            detail.setFileLocation(location);
            detail.setStatus(extension.equals(".ps") ? "failed" : "OK");
            detail.setEncrypted(isEncrypted(location, extension));
            detail.setType(determineType(location, extension));

            if (location.contains("mobstat")) mobstat.add(customerId);
            if (location.contains("archive")) archived.add(customerId);
            if (location.contains("email")) emailed.add(customerId);
            if (extension.equals(".ps")) printed.add(customerId);

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

        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("totalCustomersProcessed", customerSummaries.size());
        totals.put("totalArchived", archived.size());
        totals.put("totalEmailed", emailed.size());
        totals.put("totalMobstat", mobstat.size());
        totals.put("totalPrint", printed.size());

        // ✅ Create SummaryFileInfo object and write to C:\summary.json
        SummaryFileInfo summary = new SummaryFileInfo();
        summary.setFileName(fileName);
        summary.setJobName(jobName);
        summary.setBatchId(batchId);
        summary.setTimestamp(new Date().toString());
        summary.setCustomers(customerSummaries);
        summary.setSummaryFileURL(sasUrl);

        try {
            File jsonFile = new File("C:\\summary.json");
            objectMapper.writeValue(jsonFile, summary);
            logger.info("✅ summary.json written to C: drive");
        } catch (IOException e) {
            logger.error("Failed to write summary.json to C drive", e);
            return generateErrorResponse("601", "Failed to write summary file");
        }

        // ✅ Publish message to Kafka
        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", fileName);
        kafkaMsg.put("jobName", jobName);
        kafkaMsg.put("batchId", batchId);
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("summaryFileURL", sasUrl);
        kafkaTemplate.send(outputTopic, batchId, objectMapper.writeValueAsString(kafkaMsg));

        logger.info("Kafka message sent to topic: {}", outputTopic);

        // ✅ Return only metadata
        Map<String, Object> response = new HashMap<>();
        response.put("fileName", fileName);
        response.put("jobName", jobName);
        response.put("batchId", batchId);
        response.put("timestamp", new Date().toString());
        response.put("summaryFileURL", sasUrl);
        return response;
    }

    private boolean isEncrypted(String path, String ext) {
        return (ext.equals(".pdf") || ext.equals(".html") || ext.equals(".txt"))
                && (path.toLowerCase().contains("mobstat") || path.toLowerCase().contains("email"));
    }

    private String determineType(String path, String ext) {
        if (path.contains("mobstat")) return "pdf_mobstat";
        if (path.contains("archive")) return "pdf_archive";
        if (path.contains("email")) {
            if (ext.equals(".html")) return "html_email";
            if (ext.equals(".txt")) return "txt_email";
            return "pdf_email";
        }
        if (ext.equals(".ps")) return "ps_print";
        return "unknown";
    }

    private String getFileExtension(String path) {
        int dot = path.lastIndexOf('.');
        return dot > 0 ? path.substring(dot) : "";
    }

    private String extractField(JsonNode node, String name) {
        try {
            JsonNode val = node.get(name);
            return val != null ? val.asText() : null;
        } catch (Exception e) {
            logger.error("Failed to extract field {}: {}", name, e.getMessage());
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

    private Map<String, Object> generateErrorResponse(String status, String message) {
        Map<String, Object> error = new HashMap<>();
        error.put("status", status);
        error.put("message", message);
        return error;
    }
}
