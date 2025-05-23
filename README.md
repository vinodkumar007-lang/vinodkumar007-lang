package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import com.nedbank.kafka.filemanage.model.MetaDataInfo;
import com.nedbank.kafka.filemanage.model.PayloadInfo;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
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

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    /**
     * Called from controller to poll & process all recent Kafka messages from input topic.
     */
    public Map<String, Object> listen() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        try {
            // Get all partitions for the input topic
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            );

            consumer.assign(partitions);
            // Start from beginning of each partition
            consumer.seekToBeginning(partitions);

            List<String> recentMessages = new ArrayList<>();
            int emptyPollCount = 0;
            long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

            // Poll until 3 consecutive empty polls (no new messages)
            while (emptyPollCount < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPollCount++;
                } else {
                    emptyPollCount = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        // Only include messages from last 3 days (timestamp in millis)
                        if (record.timestamp() >= threeDaysAgo) {
                            logger.info("Received message (offset={}): {}", record.offset(), record.value());
                            recentMessages.add(record.value());
                        } else {
                            logger.debug("Skipping old message (timestamp={}): {}", record.timestamp(), record.value());
                        }
                    }
                }
            }

            if (recentMessages.isEmpty()) {
                return generateErrorResponse("204", "No recent messages found in Kafka topic.");
            }

            // Process all collected messages
            SummaryPayload summaryPayload = processMessages(recentMessages);
            File jsonFile = writeSummaryToFile(summaryPayload);
            sendFinalResponseToKafka(summaryPayload, jsonFile);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Batch processed successfully.");
            response.put("summaryPayload", summaryPayload);
            return response;

        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages.");
        } finally {
            consumer.close();
        }
    }

    /**
     * Processes all Kafka messages and builds SummaryPayload.
     */
    private SummaryPayload processMessages(List<String> allMessages) throws IOException {
        List<CustomerSummary> customerSummaries = new ArrayList<>();
        String jobName = "";
        String batchId = null;

        for (String message : allMessages) {
            JsonNode root = objectMapper.readTree(message);

            if (batchId == null) {
                batchId = safeGetText(root, "BatchId", true);
            }

            JsonNode batchFilesNode = root.get("BatchFiles");
            if (batchFilesNode == null || !batchFilesNode.isArray()) {
                logger.warn("No 'BatchFiles' array found in message.");
                continue;
            }

            for (JsonNode fileNode : batchFilesNode) {
                String filePath = safeGetText(fileNode, "BlobUrl", true);
                String objectId = safeGetText(fileNode, "ObjectId", true);
                String validationStatus = safeGetText(fileNode, "ValidationStatus", false);

                if (filePath == null || objectId == null) {
                    logger.warn("Skipping file with missing BlobUrl or ObjectId.");
                    continue;
                }

                try {
                    // Upload file to blob storage and generate SAS URL
                    blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
                } catch (Exception e) {
                    logger.warn("Blob upload failed for {}: {}", filePath, e.getMessage());
                }

                String extension = getFileExtension(filePath);
                CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
                detail.setObjectId(objectId);
                detail.setFileLocation(filePath);
                detail.setFileUrl("https://" + azureBlobStorageAccount + "/" + filePath);
                detail.setEncrypted(isEncrypted(filePath, extension));
                detail.setStatus(validationStatus != null ? validationStatus : "OK");
                detail.setType(determineType(filePath));

                CustomerSummary customer = customerSummaries.stream()
                        .filter(c -> c.getCustomerId().equals(objectId))
                        .findFirst()
                        .orElseGet(() -> {
                            CustomerSummary c = new CustomerSummary();
                            c.setCustomerId(objectId);
                            c.setAccountNumber("");
                            c.setFiles(new ArrayList<>());
                            customerSummaries.add(c);
                            return c;
                        });

                customer.getFiles().add(detail);
            }

            if (jobName.isEmpty()) {
                jobName = safeGetText(root, "JobName", false);
            }
        }

        JsonNode firstMessageRoot = objectMapper.readTree(allMessages.get(0));
        HeaderInfo headerInfo = buildHeader(firstMessageRoot, jobName);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);

        PayloadInfo payloadInfo = new PayloadInfo();
        payloadInfo.setPrintFiles(Collections.emptyList());
        summaryPayload.setPayload(payloadInfo);

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);
        summaryPayload.setMetadata(metaDataInfo);

        return summaryPayload;
    }

    private HeaderInfo buildHeader(JsonNode root, String jobName) {
        HeaderInfo headerInfo = new HeaderInfo();
        headerInfo.setBatchId(safeGetText(root, "BatchId", true));
        headerInfo.setTenantCode(safeGetText(root, "TenantCode", false));
        headerInfo.setChannelID(safeGetText(root, "ChannelID", false));
        headerInfo.setAudienceID(safeGetText(root, "AudienceID", false));
        headerInfo.setSourceSystem(safeGetText(root, "SourceSystem", false));
        headerInfo.setProduct(safeGetText(root, "Product", false));
        headerInfo.setJobName(jobName != null ? jobName : "");
        headerInfo.setTimestamp(new Date().toString());
        return headerInfo;
    }

    private File writeSummaryToFile(SummaryPayload summaryPayload) throws IOException {
        String userHome = System.getProperty("user.home");
        File jsonFile = new File(userHome, "summary.json");
        // Write a simple JSON summary with header and metadata (you can customize format)
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, Map.of(
                "batchId", summaryPayload.getHeader().getBatchId(),
                "jobName", summaryPayload.getHeader().getJobName(),
                "header", summaryPayload.getHeader(),
                "metadata", summaryPayload.getMetadata()
        ));
        summaryPayload.setSummaryFileURL(jsonFile.getAbsolutePath());
        summaryPayload.getMetadata().setSummaryFileURL(jsonFile.getAbsolutePath());
        return jsonFile;
    }

    private void sendFinalResponseToKafka(SummaryPayload summaryPayload, File jsonFile) throws IOException {
        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", summaryPayload.getHeader().getJobName());
        kafkaMsg.put("jobName", summaryPayload.getHeader().getJobName());
        kafkaMsg.put("batchId", summaryPayload.getHeader().getBatchId());
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("summaryFileURL", jsonFile.getAbsolutePath());

        String message = objectMapper.writeValueAsString(kafkaMsg);
        kafkaTemplate.send(outputTopic, summaryPayload.getHeader().getBatchId(), message);
        logger.info("Sent summary message to Kafka output topic: {}", message);
    }

    /**
     * Helper: safely get text field from JSON node, null if missing (unless required).
     */
    private String safeGetText(JsonNode node, String fieldName, boolean required) {
        if (node != null && node.has(fieldName) && !node.get(fieldName).isNull()) {
            String val = node.get(fieldName).asText();
            return "null".equalsIgnoreCase(val) ? null : val;
        }
        if (required) {
            logger.warn("Required field '{}' missing or null in message", fieldName);
        }
        return null;
    }

    private String getFileExtension(String path) {
        int lastDot = path.lastIndexOf('.');
        if (lastDot > 0 && lastDot < path.length() - 1) {
            return path.substring(lastDot + 1).toLowerCase();
        }
        return "";
    }

    private boolean isEncrypted(String filePath, String extension) {
        // Dummy encryption check, implement your logic
        return filePath.contains("encrypted") || "enc".equals(extension);
    }

    private String determineType(String filePath) {
        // Example: determine type based on extension or path
        String ext = getFileExtension(filePath);
        switch (ext) {
            case "csv": return "CSV";
            case "pdf": return "PDF";
            default: return "UNKNOWN";
        }
    }

    private Map<String, Object> generateErrorResponse(String code, String message) {
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "error");
        resp.put("code", code);
        resp.put("message", message);
        return resp;
    }
}
