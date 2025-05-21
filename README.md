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
        } finally {
            consumer.close();
        }

        return summaryResponse;
    }

    private Map<String, Object> handleMessage(String message) throws Exception {
        JsonNode root;

        System.out.println("Message check   " + message);
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

        // Simulate Blob Storage URL
        String blobUrl = "file://./output";

        // Generate summary with new format
        Map<String, Object> summaryResponse = buildSummaryPayload(batchId, blobUrl, batchFilesNode);

        String summaryMessage = objectMapper.writeValueAsString(summaryResponse);
        assert batchId != null;
        kafkaTemplate.send(outputTopic, batchId, summaryMessage);
        logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

        return summaryResponse;
    }

    private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl, JsonNode batchFilesNode) {
        List<ProcessedFileInfo> processedFiles = new ArrayList<>();
        List<CustomerSummary> customerSummaries = new ArrayList<>();

        Set<String> archivedCustomers = new HashSet<>();
        Set<String> emailedCustomers = new HashSet<>();
        Set<String> mobstatCustomers = new HashSet<>();
        Set<String> printCustomers = new HashSet<>();

        String fileName = "";
        String jobName = "";
        String tenantCode = "ZANBL"; // Can be extracted dynamically if present
        String channelId = "100";
        String sourceSystem = "CARD";
        String product = "CASA";
        String audienceId = UUID.randomUUID().toString();
        String uniqueConsumerRef = UUID.randomUUID().toString();
        String uniqueECPBatchRef = UUID.randomUUID().toString();
        String repositoryId = "Legacy";
        String runPriority = "High";
        String eventType = "Completion";
        String eventId = "E12345";
        String restartKey = "Key_" + batchId;

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

            processedFiles.add(new ProcessedFileInfo(customerId, dynamicFileUrl));

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

        // Create full detailed summary for file output
        Map<String, Object> fullSummary = new LinkedHashMap<>();
        fullSummary.put("fileName", fileName);
        fullSummary.put("jobName", jobName);
        fullSummary.put("batchId", batchId);
        fullSummary.put("timestamp", new Date().toString());
        fullSummary.put("customers", customerSummaries);

        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("totalCustomersProcessed", customerSummaries.size());
        totals.put("totalArchived", archivedCustomers.size());
        totals.put("totalEmailed", emailedCustomers.size());
        totals.put("totalMobstat", mobstatCustomers.size());
        totals.put("totalPrint", printCustomers.size());

        fullSummary.put("totals", totals);

        // Save to file locally
        String summaryFileUrl = null;
        try {
            String userHome = System.getProperty("user.home");
            File outputDir = new File(userHome + File.separator + "summary_outputs");
            if (!outputDir.exists()) outputDir.mkdirs();

            String filePath = outputDir + File.separator + "summary_" + batchId + ".json";
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(filePath), fullSummary);
            logger.info("Summary JSON written to local file: {}", filePath);

            // Open in default viewer (Windows only)
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                Runtime.getRuntime().exec(new String[]{"cmd", "/c", "start", "", filePath});
            }

            summaryFileUrl = "file://" + filePath.replace("\\", "/");

        } catch (IOException e) {
            logger.error("Failed to write local summary JSON file", e);
        }

        // Final summary to return in API/Kafka
        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(batchId);
        summary.setHeader(new HeaderInfo(tenantCode, channelId, audienceId, new Date(), sourceSystem, product, jobName));
        summary.setMetadata(new MetadataInfo(
                customerSummaries.size(), // total files/customers processed
                "Success",
                "Success",
                "All customer PDFs processed successfully"
        ));
        summary.setPayload(new PayloadInfo(
                uniqueConsumerRef,
                uniqueECPBatchRef,
                List.of(uniqueECPBatchRef), // FilenetObjectID
                repositoryId,
                runPriority,
                eventId,
                eventType,
                restartKey
        ));
        summary.setProcessedFiles(processedFiles);
        summary.setSummaryFileURL(summaryFileUrl);
        summary.setTimestamp(new Date());

        return objectMapper.convertValue(summary, Map.class);
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
            throw new RuntimeException("Failed to extract " + fieldName, e);
        }
    }

    private String convertPojoToJson(String raw) {
        raw = raw.trim();
        if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
            raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
        }
        raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
        raw = raw.replaceAll(":([a-zA-Z0-9_]+)", ":\"$1\"");
        raw = raw.replaceAll("objectId=\\{([^}]+)}", "\"objectId\": \"$1\"");
        raw = raw.replace("],", "], ");
        return "{" + raw + "}";
    }

    public PublishEvent parseToPublishEvent(String raw) {
        try {
            if (raw.startsWith("PublishEvent(")) {
                raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
            }
            raw = raw.replaceAll("BatchFile\\(", "\\{").replaceAll("\\)", "}");
            raw = raw.replaceAll("objectId=\\{([^}]+)}", "objectId:\"$1\"");
            raw = raw.replaceAll("(\\w+)=([^,\\[{]+)", "\"$1\":\"$2\"");
            raw = raw.replace("\"batchFiles\":\"[", "\"batchFiles\":[");
            raw = raw.replace("]\"", "]");
            String json = "{" + raw + "}";
            ObjectMapper mapper = new ObjectMapper();
            mapper.findAndRegisterModules();
            return mapper.readValue(json, PublishEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse to PublishEvent", e);
        }
    }
}
