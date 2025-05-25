package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import com.nedbank.kafka.filemanage.model.MetaDataInfo;
import com.nedbank.kafka.filemanage.model.PayloadInfo;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
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
            long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
                    partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            );
            consumer.assign(partitions);

            // Seek to offset based on timestamp (3 days ago)
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartition partition : partitions) {
                timestampsToSearch.put(partition, threeDaysAgo);
            }

            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);
            for (TopicPartition partition : partitions) {
                OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
                if (offsetAndTimestamp != null) {
                    consumer.seek(partition, offsetAndTimestamp.offset());
                } else {
                    consumer.seekToBeginning(Collections.singletonList(partition));
                }
            }

            List<SummaryPayload> processedPayloads = new ArrayList<>();
            int emptyPollCount = 0;

            while (emptyPollCount < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPollCount++;
                } else {
                    emptyPollCount = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Processing message (offset={}): {}", record.offset(), record.value());
                        try {
                            SummaryPayload summaryPayload = processSingleMessage(record.value());
                            SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);
                            processedPayloads.add(summaryPayload);
                        } catch (Exception ex) {
                            logger.error("Failed to process message: {}", ex.getMessage(), ex);
                        }
                    }
                }
            }

            if (processedPayloads.isEmpty()) {
                return generateErrorResponse("204", "No recent messages found in Kafka topic.");
            }

            SummaryPayload finalSummary = mergeSummaryPayloads(processedPayloads);
            String finalSummaryJson = objectMapper.writeValueAsString(finalSummary);

            kafkaTemplate.send(outputTopic, finalSummaryJson);
            logger.info("Final combined summary sent to topic: {}", outputTopic);

            return buildFinalResponse(finalSummary);

        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages.");
        } finally {
            consumer.close();
        }
    }

    private SummaryPayload processSingleMessage(String message) throws IOException {
        JsonNode root = objectMapper.readTree(message);

        // Extract jobName (optional)
        String jobName = safeGetText(root, "JobName", false);
        if (jobName == null) jobName = "";

        // Extract BatchId (mandatory fallback to random UUID)
        String batchId = safeGetText(root, "BatchId", true);
        if (batchId == null) batchId = UUID.randomUUID().toString();

        // Process CustomerSummaries from BatchFiles array
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

                // Find existing customer or create new
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

        // Build Header info - check if nested "Header" node exists, else fallback to root
        HeaderInfo headerInfo = null;
        JsonNode headerNode = root.get("Header");
        if (headerNode != null && !headerNode.isNull()) {
            headerInfo = buildHeader(headerNode, jobName);
        } else {
            headerInfo = buildHeader(root, jobName);
        }

        if (headerInfo.getBatchId() == null) {
            headerInfo.setBatchId(batchId);
        }
        
        // NEW: extract product field if present in header or root
        String product = null;
        if (headerNode != null && headerNode.has("product")) {
            product = safeGetText(headerNode, "product", false);
        }
        if (product == null) {
            product = safeGetText(root, "product", false);
        }
        headerInfo.setProduct(product);

        // Build Payload info - check if nested "Payload" node exists, else fallback to root
        PayloadInfo payloadInfo = new PayloadInfo();
        JsonNode payloadNode = root.get("Payload");
        if (payloadNode != null && !payloadNode.isNull()) {
            payloadInfo.setUniqueConsumerRef(safeGetText(payloadNode, "uniqueConsumerRef", false));
            payloadInfo.setUniqueECPBatchRef(safeGetText(payloadNode, "uniqueECPBatchRef", false));
            payloadInfo.setRunPriority(safeGetText(payloadNode, "runPriority", false));
            payloadInfo.setEventID(safeGetText(payloadNode, "eventID", false));
            payloadInfo.setEventType(safeGetText(payloadNode, "eventType", false));
            payloadInfo.setRestartKey(safeGetText(payloadNode, "restartKey", false));
            payloadInfo.setBlobURL(safeGetText(payloadNode, "blobURL", false)); // NEW
            payloadInfo.setEventOutcomeCode(safeGetText(payloadNode, "eventOutcomeCode", false)); // NEW
            payloadInfo.setEventOutcomeDescription(safeGetText(payloadNode, "eventOutcomeDescription", false)); // NEW
            
            // Assume PrintFiles is a list of strings if present
            JsonNode printFilesNode = payloadNode.get("printFiles");
            if (printFilesNode != null && printFilesNode.isArray()) {
                List<String> printFiles = new ArrayList<>();
                for (JsonNode pf : printFilesNode) {
                    printFiles.add(pf.asText());
                }
                payloadInfo.setPrintFiles(printFiles);
            }
        }

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setTotalFiles(customerSummaries.stream().mapToInt(c -> c.getFiles().size()).sum());
        metaDataInfo.setTotalCustomers(customerSummaries.size());

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setJobName(jobName);
        summaryPayload.setBatchId(batchId);
        summaryPayload.setCustomerSummary(customerSummaries);
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setMetaData(metaDataInfo);

        return summaryPayload;
    }

    private HeaderInfo buildHeader(JsonNode node, String jobName) {
        HeaderInfo headerInfo = new HeaderInfo();
        headerInfo.setBatchId(safeGetText(node, "BatchId", false));
        headerInfo.setRunPriority(safeGetText(node, "RunPriority", false));
        headerInfo.setEventID(safeGetText(node, "EventID", false));
        headerInfo.setEventType(safeGetText(node, "EventType", false));
        headerInfo.setRestartKey(safeGetText(node, "RestartKey", false));
        headerInfo.setJobName(jobName);
        return headerInfo;
    }

    private boolean isEncrypted(String filePath, String extension) {
        // Your encryption logic here, e.g.:
        return filePath.endsWith(".enc") || "enc".equalsIgnoreCase(extension);
    }

    private String determineType(String filePath) {
        // Your type logic here, e.g.:
        if (filePath.endsWith(".pdf")) return "PDF";
        if (filePath.endsWith(".xml")) return "XML";
        return "UNKNOWN";
    }

    private String getFileExtension(String filePath) {
        if (filePath == null) return "";
        int lastDot = filePath.lastIndexOf('.');
        if (lastDot < 0) return "";
        return filePath.substring(lastDot + 1);
    }

    private String safeGetText(JsonNode node, String fieldName, boolean mandatory) {
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) {
            if (mandatory) {
                logger.warn("Missing mandatory field '{}'", fieldName);
            }
            return null;
        }
        return child.asText();
    }

    private SummaryPayload mergeSummaryPayloads(List<SummaryPayload> payloads) {
        if (payloads.isEmpty()) return new SummaryPayload();

        SummaryPayload merged = new SummaryPayload();
        List<CustomerSummary> allCustomers = new ArrayList<>();
        MetaDataInfo metaData = new MetaDataInfo();

        String jobName = payloads.get(0).getJobName();
        String batchId = payloads.get(0).getBatchId();

        for (SummaryPayload p : payloads) {
            allCustomers.addAll(p.getCustomerSummary());
        }

        merged.setJobName(jobName);
        merged.setBatchId(batchId);
        merged.setCustomerSummary(allCustomers);
        merged.setHeader(payloads.get(0).getHeader());
        merged.setPayload(payloads.get(0).getPayload());

        metaData.setTotalCustomers(allCustomers.size());
        metaData.setTotalFiles(allCustomers.stream().mapToInt(c -> c.getFiles().size()).sum());
        merged.setMetaData(metaData);

        return merged;
    }

    private Map<String, Object> buildFinalResponse(SummaryPayload summaryPayload) {
        Map<String, Object> response = new HashMap<>();

        response.put("JobName", summaryPayload.getJobName());
        response.put("BatchId", summaryPayload.getBatchId());
        response.put("CustomerSummary", summaryPayload.getCustomerSummary());
        response.put("MetaData", summaryPayload.getMetaData());

        // Add Header info
        Map<String, Object> headerMap = new HashMap<>();
        HeaderInfo header = summaryPayload.getHeader();
        if (header != null) {
            headerMap.put("BatchId", header.getBatchId());
            headerMap.put("RunPriority", header.getRunPriority());
            headerMap.put("EventID", header.getEventID());
            headerMap.put("EventType", header.getEventType());
            headerMap.put("RestartKey", header.getRestartKey());
            headerMap.put("JobName", header.getJobName());
            headerMap.put("Product", header.getProduct());  // NEW field
        }
        response.put("Header", headerMap);

        // Add Payload info
        Map<String, Object> payloadMap = new HashMap<>();
        PayloadInfo payload = summaryPayload.getPayload();
        if (payload != null) {
            payloadMap.put("uniqueConsumerRef", payload.getUniqueConsumerRef());
            payloadMap.put("uniqueECPBatchRef", payload.getUniqueECPBatchRef());
            payloadMap.put("runPriority", payload.getRunPriority());
            payloadMap.put("eventID", payload.getEventID());
            payloadMap.put("eventType", payload.getEventType());
            payloadMap.put("restartKey", payload.getRestartKey());
            payloadMap.put("blobURL", payload.getBlobURL());                     // NEW
            payloadMap.put("eventOutcomeCode", payload.getEventOutcomeCode());   // NEW
            payloadMap.put("eventOutcomeDescription", payload.getEventOutcomeDescription()); // NEW
            payloadMap.put("printFiles", payload.getPrintFiles());
        }
        response.put("Payload", payloadMap);

        return response;
    }

    private Map<String, Object> generateErrorResponse(String code, String message) {
        Map<String, Object> error = new HashMap<>();
        error.put("code", code);
        error.put("message", message);
        return error;
    }

    // Keeping your other existing methods (like uploadSummaryToBlob, sendToKafka, etc.) unchanged...
}
