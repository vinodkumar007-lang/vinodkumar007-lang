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

            List<SummaryPayload> processedPayloads = new ArrayList<>();
            for (String message : recentMessages) {
                SummaryPayload summaryPayload = processSingleMessage(message);
                SummaryJsonWriter.writeUpdatedSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);
                processedPayloads.add(summaryPayload);
            }

            SummaryPayload finalSummary = mergeSummaryPayloads(processedPayloads);

            String finalSummaryJson = objectMapper.writeValueAsString(finalSummary);

            kafkaTemplate.send(outputTopic, finalSummaryJson);
            logger.info("Final combined summary sent to topic: {}", outputTopic);

            // Build response map as requested
            Map<String, Object> responseMap = buildFinalResponse(finalSummary);
            return responseMap;

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
            // Assume PrintFiles is a list of strings if present
            JsonNode printFilesNode = payloadNode.get("printFiles");
            if (printFilesNode != null && printFilesNode.isArray()) {
                List<String> printFiles = new ArrayList<>();
                for (JsonNode pf : printFilesNode) {
                    printFiles.add(pf.asText());
                }
                payloadInfo.setPrintFiles(printFiles);
            } else {
                payloadInfo.setPrintFiles(Collections.emptyList());
            }
        } else {
            // fallback to root level keys if any
            payloadInfo.setUniqueConsumerRef(safeGetText(root, "uniqueConsumerRef", false));
            payloadInfo.setUniqueECPBatchRef(safeGetText(root, "uniqueECPBatchRef", false));
            payloadInfo.setRunPriority(safeGetText(root, "runPriority", false));
            payloadInfo.setEventID(safeGetText(root, "eventID", false));
            payloadInfo.setEventType(safeGetText(root, "eventType", false));
            payloadInfo.setRestartKey(safeGetText(root, "restartKey", false));
            payloadInfo.setPrintFiles(Collections.emptyList());
        }

        // Metadata
        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setMetaData(metaDataInfo);

        return summaryPayload;
    }

    private HeaderInfo buildHeader(JsonNode node, String jobName) {
        HeaderInfo header = new HeaderInfo();
        header.setJobName(jobName != null ? jobName : safeGetText(node, "JobName", false));
        header.setBatchId(safeGetText(node, "BatchId", false));
        header.setBatchStatus(safeGetText(node, "BatchStatus", false));
        header.setSourceSystem(safeGetText(node, "SourceSystem", false));
        header.setQueueName(safeGetText(node, "QueueName", false));
        return header;
    }

    private boolean isEncrypted(String filePath, String extension) {
        // Sample logic, update with your encryption check
        return extension.equalsIgnoreCase("enc");
    }

    private String getFileExtension(String filePath) {
        if (filePath == null || !filePath.contains(".")) {
            return "";
        }
        return filePath.substring(filePath.lastIndexOf('.') + 1);
    }

    private String determineType(String filePath) {
        if (filePath == null) return "";
        String ext = getFileExtension(filePath).toLowerCase();
        switch (ext) {
            case "pdf": return "PDF";
            case "doc":
            case "docx": return "DOC";
            case "enc": return "Encrypted";
            default: return "Unknown";
        }
    }

    private Map<String, Object> buildFinalResponse(SummaryPayload finalSummary) {
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("batchId", finalSummary.getHeader().getBatchId());
        responseMap.put("batchStatus", finalSummary.getHeader().getBatchStatus());
        responseMap.put("jobName", finalSummary.getHeader().getJobName());

        List<Map<String, Object>> customers = new ArrayList<>();
        for (CustomerSummary cs : finalSummary.getMetaData().getCustomerSummaries()) {
            Map<String, Object> custMap = new HashMap<>();
            custMap.put("customerId", cs.getCustomerId());
            custMap.put("accountNumber", cs.getAccountNumber());

            List<Map<String, Object>> filesList = new ArrayList<>();
            for (CustomerSummary.FileDetail fd : cs.getFiles()) {
                Map<String, Object> fileMap = new HashMap<>();
                fileMap.put("objectId", fd.getObjectId());
                fileMap.put("fileLocation", fd.getFileLocation());
                fileMap.put("fileUrl", fd.getFileUrl());
                fileMap.put("encrypted", fd.isEncrypted());
                fileMap.put("status", fd.getStatus());
                fileMap.put("type", fd.getType());
                filesList.add(fileMap);
            }
            custMap.put("files", filesList);
            customers.add(custMap);
        }
        responseMap.put("customerSummaries", customers);
        responseMap.put("uniqueConsumerRef", finalSummary.getPayload().getUniqueConsumerRef());
        responseMap.put("uniqueECPBatchRef", finalSummary.getPayload().getUniqueECPBatchRef());
        responseMap.put("runPriority", finalSummary.getPayload().getRunPriority());
        responseMap.put("eventID", finalSummary.getPayload().getEventID());
        responseMap.put("eventType", finalSummary.getPayload().getEventType());
        responseMap.put("restartKey", finalSummary.getPayload().getRestartKey());
        responseMap.put("printFiles", finalSummary.getPayload().getPrintFiles());

        return responseMap;
    }

    private SummaryPayload mergeSummaryPayloads(List<SummaryPayload> payloads) {
        if (payloads == null || payloads.isEmpty()) {
            return new SummaryPayload();
        }

        SummaryPayload merged = new SummaryPayload();

        // Merge headers - pick first non-null header or combine if needed
        HeaderInfo mergedHeader = new HeaderInfo();
        for (SummaryPayload p : payloads) {
            if (p.getHeader() != null) {
                if (mergedHeader.getBatchId() == null)
                    mergedHeader.setBatchId(p.getHeader().getBatchId());
                if (mergedHeader.getBatchStatus() == null)
                    mergedHeader.setBatchStatus(p.getHeader().getBatchStatus());
                if (mergedHeader.getJobName() == null)
                    mergedHeader.setJobName(p.getHeader().getJobName());
                if (mergedHeader.getSourceSystem() == null)
                    mergedHeader.setSourceSystem(p.getHeader().getSourceSystem());
                if (mergedHeader.getQueueName() == null)
                    mergedHeader.setQueueName(p.getHeader().getQueueName());
            }
        }
        merged.setHeader(mergedHeader);

        // Merge payloads - for simplicity pick first non-null uniqueConsumerRef, etc.
        PayloadInfo mergedPayload = new PayloadInfo();
        for (SummaryPayload p : payloads) {
            PayloadInfo pl = p.getPayload();
            if (pl != null) {
                if (mergedPayload.getUniqueConsumerRef() == null)
                    mergedPayload.setUniqueConsumerRef(pl.getUniqueConsumerRef());
                if (mergedPayload.getUniqueECPBatchRef() == null)
                    mergedPayload.setUniqueECPBatchRef(pl.getUniqueECPBatchRef());
                if (mergedPayload.getRunPriority() == null)
                    mergedPayload.setRunPriority(pl.getRunPriority());
                if (mergedPayload.getEventID() == null)
                    mergedPayload.setEventID(pl.getEventID());
                if (mergedPayload.getEventType() == null)
                    mergedPayload.setEventType(pl.getEventType());
                if (mergedPayload.getRestartKey() == null)
                    mergedPayload.setRestartKey(pl.getRestartKey());
            }
        }

        // Merge printFiles lists, flatten all
        List<String> allPrintFiles = new ArrayList<>();
        for (SummaryPayload p : payloads) {
            PayloadInfo pl = p.getPayload();
            if (pl != null && pl.getPrintFiles() != null) {
                allPrintFiles.addAll(pl.getPrintFiles());
            }
        }
        mergedPayload.setPrintFiles(allPrintFiles);

        merged.setPayload(mergedPayload);

        // Merge metadata: combine all customer summaries into one list (merge if needed by customerId)
        Map<String, CustomerSummary> mergedCustomers = new HashMap<>();
        for (SummaryPayload p : payloads) {
            if (p.getMetaData() != null && p.getMetaData().getCustomerSummaries() != null) {
                for (CustomerSummary cs : p.getMetaData().getCustomerSummaries()) {
                    if (cs == null || cs.getCustomerId() == null) continue;

                    CustomerSummary existing = mergedCustomers.get(cs.getCustomerId());
                    if (existing == null) {
                        mergedCustomers.put(cs.getCustomerId(), cs);
                    } else {
                        // Merge files list for this customer
                        if (cs.getFiles() != null) {
                            if (existing.getFiles() == null) {
                                existing.setFiles(new ArrayList<>());
                            }
                            existing.getFiles().addAll(cs.getFiles());
                        }
                    }
                }
            }
        }

        MetaDataInfo mergedMetaData = new MetaDataInfo();
        mergedMetaData.setCustomerSummaries(new ArrayList<>(mergedCustomers.values()));
        merged.setMetaData(mergedMetaData);

        return merged;
    }

    private Map<String, Object> generateErrorResponse(String code, String message) {
        Map<String, Object> errResp = new HashMap<>();
        errResp.put("code", code);
        errResp.put("message", message);
        return errResp;
    }

    private String safeGetText(JsonNode node, String fieldName, boolean required) {
        if (node == null || !node.has(fieldName)) {
            if (required) {
                logger.warn("Required field '{}' missing in JSON", fieldName);
            }
            return null;
        }
        JsonNode valueNode = node.get(fieldName);
        if (valueNode == null || valueNode.isNull()) {
            if (required) {
                logger.warn("Required field '{}' is null in JSON", fieldName);
            }
            return null;
        }
        return valueNode.asText();
    }
}
