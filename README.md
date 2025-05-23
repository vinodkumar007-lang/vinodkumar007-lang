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
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
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

    private final File summaryFile = new File(System.getProperty("user.home"), "summary.json");

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public Map<String, Object> listen() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        try {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
                    partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            );

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            List<String> recentMessages = new ArrayList<>();
            int emptyPollCount = 0;
            long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

            while (emptyPollCount < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPollCount++;
                } else {
                    emptyPollCount = 0;
                    for (ConsumerRecord<String, String> record : records) {
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

            // Process each message individually and append/update summary.json
            List<SummaryPayload> processedPayloads = new ArrayList<>();
            for (String message : recentMessages) {
                SummaryPayload summaryPayload = processSingleMessage(message);
                appendSummaryToFile(summaryPayload);
                processedPayloads.add(summaryPayload);
            }

            // Combine all SummaryPayloads into one final summary
            SummaryPayload finalSummary = mergeSummaryPayloads(processedPayloads);

            // Serialize final summary to JSON string and send to output Kafka topic
            String finalSummaryJson = objectMapper.writeValueAsString(finalSummary);
            kafkaTemplate.send(outputTopic, finalSummaryJson);
            logger.info("Final combined summary sent to topic: {}", outputTopic);

            // Return the final summary as Map (for your REST response)
            return objectMapper.convertValue(finalSummary, Map.class);

        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages.");
        } finally {
            consumer.close();
        }
    }

    /**
     * Merge multiple SummaryPayload objects into one combined SummaryPayload.
     * Adjust merging logic as needed.
     */
    private SummaryPayload mergeSummaryPayloads(List<SummaryPayload> payloads) {
        if (payloads.isEmpty()) {
            return new SummaryPayload();
        }
        SummaryPayload merged = new SummaryPayload();

        // Merge Headers - example: take first or combine fields intelligently
        HeaderInfo mergedHeader = new HeaderInfo();
        // You can customize this; here we take first payload's header for simplicity
        mergedHeader = payloads.get(0).getHeader();
        merged.setHeader(mergedHeader);

        // Merge PayloadInfo - you can customize merging strategy here
        PayloadInfo mergedPayloadInfo = new PayloadInfo();
        List<Object> combinedPrintFiles = new ArrayList<>();
        for (SummaryPayload sp : payloads) {
            if (sp.getPayload() != null && sp.getPayload().getPrintFiles() != null) {
                combinedPrintFiles.addAll(sp.getPayload().getPrintFiles());
            }
        }
        mergedPayloadInfo.setPrintFiles(combinedPrintFiles);
        merged.setPayload(mergedPayloadInfo);

        // Merge MetadataInfo (customer summaries)
        MetaDataInfo mergedMeta = new MetaDataInfo();
        List<CustomerSummary> combinedCustomers = new ArrayList<>();
        for (SummaryPayload sp : payloads) {
            if (sp.getMetadata() != null && sp.getMetadata().getCustomerSummaries() != null) {
                combinedCustomers.addAll(sp.getMetadata().getCustomerSummaries());
            }
        }
        mergedMeta.setCustomerSummaries(combinedCustomers);
        merged.setMetadata(mergedMeta);

        return merged;
    }

    /**
     * Process a single Kafka message JSON string and build a SummaryPayload.
     */
    private SummaryPayload processSingleMessage(String message) throws IOException {
        JsonNode root = objectMapper.readTree(message);

        String jobName = safeGetText(root, "JobName", false);
        if (jobName == null) jobName = "";  // Null-safe default

        String batchId = safeGetText(root, "BatchId", true);
        if (batchId == null) batchId = UUID.randomUUID().toString(); // fallback ID

        List<CustomerSummary> customerSummaries = new ArrayList<>();

        JsonNode batchFilesNode = root.get("BatchFiles");
        if (batchFilesNode != null && batchFilesNode.isArray()) {
            for (JsonNode fileNode : batchFilesNode) {
                String filePath = safeGetText(fileNode, "BlobUrl", true);
                String objectId = safeGetText(fileNode, "ObjectId", true);
                String validationStatus = safeGetText(fileNode, "ValidationStatus", false);

                if (filePath == null || objectId == null) {
                    logger.warn("Skipping file with missing BlobUrl or ObjectId.");
                    continue;
                }

                try {
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
        }

        HeaderInfo headerInfo = buildHeader(root, jobName);

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

    /**
     * Append or merge SummaryPayload data to summary.json
     */
    private void appendSummaryToFile(SummaryPayload newSummary) throws IOException {
        Map<String, Object> existingData = new HashMap<>();

        if (summaryFile.exists()) {
            try {
                existingData = objectMapper.readValue(summaryFile, Map.class);
            } catch (Exception e) {
                logger.warn("Failed to read existing summary.json, starting fresh.", e);
                existingData = new HashMap<>();
            }
        }

        // Merge new summary data with existing data intelligently

        // Example: store multiple headers and metadata in lists (you can adjust based on your schema)
        List<Map<String, Object>> headers = (List<Map<String, Object>>) existingData.getOrDefault("headers", new ArrayList<>());
        headers.add(objectMapper.convertValue(newSummary.getHeader(), Map.class));
        existingData.put("headers", headers);

        List<Map<String, Object>> metadatas = (List<Map<String, Object>>) existingData.getOrDefault("metadata", new ArrayList<>());
        metadatas.add(objectMapper.convertValue(newSummary.getMetadata(), Map.class));
        existingData.put("metadata", metadatas);

        // Optionally handle payload similarly if needed

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, existingData);
        logger.info("Appended new summary data to {}", summaryFile.getAbsolutePath());
    }

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
