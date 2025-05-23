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
                // Instead of old append method, now use SummaryJsonWriter helper:
                SummaryJsonWriter.writeUpdatedSummaryJson(summaryFile, summaryPayload, azureBlobStorageAccount);
                processedPayloads.add(summaryPayload);
            }

            SummaryPayload finalSummary = mergeSummaryPayloads(processedPayloads);

            String finalSummaryJson = objectMapper.writeValueAsString(finalSummary);
            kafkaTemplate.send(outputTopic, finalSummaryJson);
            logger.info("Final combined summary sent to topic: {}", outputTopic);

            return objectMapper.convertValue(finalSummary, Map.class);

        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages.");
        } finally {
            consumer.close();
        }
    }

    private SummaryPayload mergeSummaryPayloads(List<SummaryPayload> payloads) {
        if (payloads.isEmpty()) {
            return new SummaryPayload();
        }
        SummaryPayload merged = new SummaryPayload();

        HeaderInfo mergedHeader = payloads.get(0).getHeader();
        merged.setHeader(mergedHeader);

        PayloadInfo mergedPayloadInfo = new PayloadInfo();
        List<Object> combinedPrintFiles = new ArrayList<>();
        for (SummaryPayload sp : payloads) {
            if (sp.getPayload() != null && sp.getPayload().getPrintFiles() != null) {
                combinedPrintFiles.addAll(sp.getPayload().getPrintFiles());
            }
        }
        mergedPayloadInfo.setPrintFiles(combinedPrintFiles);
        merged.setPayload(mergedPayloadInfo);

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

    private SummaryPayload processSingleMessage(String message) throws IOException {
        JsonNode root = objectMapper.readTree(message);

        String jobName = safeGetText(root, "JobName", false);
        if (jobName == null) jobName = "";

        String batchId = safeGetText(root, "BatchId", true);
        if (batchId == null) batchId = UUID.randomUUID().toString();

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
        summaryPayload.setSummaryURL(summaryFile.getAbsolutePath());

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
        return filePath.contains("encrypted") || "enc".equals(extension);
    }

    private String determineType(String filePath) {
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
package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Component
public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void writeUpdatedSummaryJson(File summaryFile, SummaryPayload payload, String azureBlobStorageAccount) {
        try {
            ObjectNode rootNode = mapper.createObjectNode();

            HeaderInfo header = payload.getHeader();
            rootNode.put("batchID", header.getBatchId());
            rootNode.put("fileName", header.getJobName() + "_" + header.getBatchId() + ".csv");

            // Header info
            ObjectNode headerNode = mapper.createObjectNode();
            headerNode.put("tenantCode", header.getTenantCode());
            headerNode.put("channelID", header.getChannelID());
            headerNode.put("audienceID", header.getAudienceID());
            headerNode.put("timestamp", header.getTimestamp());
            headerNode.put("sourceSystem", header.getSourceSystem());
            headerNode.put("product", header.getProduct());
            headerNode.put("jobName", header.getJobName());
            rootNode.set("header", headerNode);

            // Processed Files
            List<ObjectNode> processedList = new ArrayList<>();
            for (CustomerSummary customer : payload.getMetadata().getCustomerSummaries()) {
                ObjectNode custNode = mapper.createObjectNode();
                custNode.put("customerID", customer.getCustomerId());
                custNode.put("accountNumber", customer.getAccountNumber() != null ? customer.getAccountNumber() : "");

                customer.getFiles().forEach(file -> {
                    String path = file.getFileLocation().toLowerCase();
                    if (path.contains("archive")) {
                        custNode.put("pdfArchiveFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    } else if (path.contains("email") && path.endsWith(".pdf")) {
                        custNode.put("pdfEmailFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    } else if (path.contains("email") && path.endsWith(".html")) {
                        custNode.put("htmlEmailFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    } else if (path.contains("email") && path.endsWith(".txt")) {
                        custNode.put("txtEmailFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    } else if (path.contains("mobstat") && path.endsWith(".pdf")) {
                        custNode.put("pdfMobstatFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    }
                });

                custNode.put("statusCode", "OK");
                custNode.put("statusDescription", "Success");
                processedList.add(custNode);
            }
            rootNode.set("processedFiles", mapper.valueToTree(processedList));

            // Print Files
            List<Map<String, String>> printFilesList = new ArrayList<>();
            if (payload.getPayload().getPrintFiles() != null) {
                for (Object printFileObj : payload.getPayload().getPrintFiles()) {
                    if (printFileObj instanceof String printFilePath) {
                        Map<String, String> map = new HashMap<>();
                        map.put("printFileURL", "https://" + azureBlobStorageAccount + "/" + printFilePath);
                        printFilesList.add(map);
                    }
                }
            }
            rootNode.set("printFiles", mapper.valueToTree(printFilesList));

            // Write to file
            mapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, rootNode);
            logger.info("Successfully wrote updated summary to {}", summaryFile.getAbsolutePath());

        } catch (IOException e) {
            logger.error("Failed to write summary.json", e);
        }
    }
}
{
    "message": "Batch processed successfully",
    "status": "success",
    "summaryPayload": {
        "batchID": null,
        "header": {
            "tenantCode": null,
            "channelID": null,
            "audienceID": null,
            "timestamp": "Thu May 22 05:44:21 SAST 2025",
            "sourceSystem": "DEBTMAN",
            "product": null,
            "jobName": ""
        },
        "metadata": {
            "totalFilesProcessed": 0,
            "processingStatus": null,
            "eventOutcomeCode": null,
            "eventOutcomeDescription": null
        },
        "payload": {
            "uniqueConsumerRef": null,
            "uniqueECPBatchRef": null,
            "filenetObjectID": null,
            "repositoryID": null,
            "runPriority": null,
            "eventID": null,
            "eventType": null,
            "restartKey": null
        },
        "processedFiles": null,
        "summaryFileURL": "C:\\Users\\CC437236\\summary.json",
        "timestamp": null
    }
}

