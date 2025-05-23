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

    private Map<String, Object> buildFinalResponse(SummaryPayload summaryPayload) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("message", "Batch processed successfully");
        response.put("status", "success");

        Map<String, Object> summaryPayloadMap = new LinkedHashMap<>();

        // batchID from header batchId or null
        String batchId = summaryPayload.getHeader() != null ? summaryPayload.getHeader().getBatchId() : null;
        summaryPayloadMap.put("batchID", batchId);

        // header map
        Map<String, Object> headerMap = new LinkedHashMap<>();
        if (summaryPayload.getHeader() != null) {
            headerMap.put("tenantCode", summaryPayload.getHeader().getTenantCode());
            headerMap.put("channelID", summaryPayload.getHeader().getChannelID());
            headerMap.put("audienceID", summaryPayload.getHeader().getAudienceID());
            headerMap.put("timestamp", summaryPayload.getHeader().getTimestamp());
            headerMap.put("sourceSystem", summaryPayload.getHeader().getSourceSystem());
            headerMap.put("product", summaryPayload.getHeader().getProduct());
            headerMap.put("jobName", summaryPayload.getHeader().getJobName());
        } else {
            headerMap.put("tenantCode", null);
            headerMap.put("channelID", null);
            headerMap.put("audienceID", null);
            headerMap.put("timestamp", null);
            headerMap.put("sourceSystem", "DEBTMAN");  // per your example default
            headerMap.put("product", null);
            headerMap.put("jobName", "");
        }
        summaryPayloadMap.put("header", headerMap);

        // metadata map
        Map<String, Object> metadataMap = new LinkedHashMap<>();
        if (summaryPayload.getMetadata() != null) {
            metadataMap.put("totalFilesProcessed", summaryPayload.getMetadata().getCustomerSummaries() != null ?
                    summaryPayload.getMetadata().getCustomerSummaries().size() : 0);
            metadataMap.put("processingStatus", summaryPayload.getMetadata().getProcessingStatus());
            metadataMap.put("eventOutcomeCode", summaryPayload.getMetadata().getEventOutcomeCode());
            metadataMap.put("eventOutcomeDescription", summaryPayload.getMetadata().getEventOutcomeDescription());
        } else {
            metadataMap.put("totalFilesProcessed", 0);
            metadataMap.put("processingStatus", null);
            metadataMap.put("eventOutcomeCode", null);
            metadataMap.put("eventOutcomeDescription", null);
        }
        summaryPayloadMap.put("metadata", metadataMap);

        // payload map
        Map<String, Object> payloadMap = new LinkedHashMap<>();
        if (summaryPayload.getPayload() != null) {
            payloadMap.put("uniqueConsumerRef", summaryPayload.getPayload().getUniqueConsumerRef());
            payloadMap.put("uniqueECPBatchRef", summaryPayload.getPayload().getUniqueECPBatchRef());
            payloadMap.put("runPriority", summaryPayload.getPayload().getRunPriority());
            payloadMap.put("eventID", summaryPayload.getPayload().getEventID());
            payloadMap.put("eventType", summaryPayload.getPayload().getEventType());
            payloadMap.put("restartKey", summaryPayload.getPayload().getRestartKey());
        } else {
            payloadMap.put("uniqueConsumerRef", null);
            payloadMap.put("uniqueECPBatchRef", null);
            payloadMap.put("runPriority", null);
            payloadMap.put("eventID", null);
            payloadMap.put("eventType", null);
            payloadMap.put("restartKey", null);
        }
        summaryPayloadMap.put("payload", payloadMap);

        // summaryFileURL and timestamp
        summaryPayloadMap.put("summaryFileURL", summaryFile.getAbsolutePath());
        summaryPayloadMap.put("timestamp", summaryPayload.getHeader() != null ? summaryPayload.getHeader().getTimestamp() : null);

        response.put("summaryPayload", summaryPayloadMap);
        return response;
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

        // Also merge all the other payload fields (take from first non-null occurrence)
        for (SummaryPayload sp : payloads) {
            PayloadInfo p = sp.getPayload();
            if (p != null) {
                if (mergedPayloadInfo.getUniqueConsumerRef() == null)
                    mergedPayloadInfo.setUniqueConsumerRef(p.getUniqueConsumerRef());
                if (mergedPayloadInfo.getUniqueECPBatchRef() == null)
                    mergedPayloadInfo.setUniqueECPBatchRef(p.getUniqueECPBatchRef());
                if (mergedPayloadInfo.getRunPriority() == null)
                    mergedPayloadInfo.setRunPriority(p.getRunPriority());
                if (mergedPayloadInfo.getEventID() == null)
                    mergedPayloadInfo.setEventID(p.getEventID());
                if (mergedPayloadInfo.getEventType() == null)
                    mergedPayloadInfo.setEventType(p.getEventType());
                if (mergedPayloadInfo.getRestartKey() == null)
                    mergedPayloadInfo.setRestartKey(p.getRestartKey());
            }
        }
        merged.setPayload(mergedPayloadInfo);

        MetaDataInfo mergedMeta = new MetaDataInfo();
        List<CustomerSummary> combinedCustomers = new ArrayList<>();
        for (SummaryPayload sp : payloads) {
            if (sp.getMetadata() != null && sp.getMetadata().getCustomerSummaries() != null) {
                combinedCustomers.addAll(sp.getMetadata().getCustomerSummaries());
            }
        }
        mergedMeta.setCustomerSummaries(combinedCustomers);
        // You might want to merge processingStatus and eventOutcomeCode/Description here as well if needed
        merged.setMetadata(mergedMeta);

        merged.setSummaryURL(summaryFile.getAbsolutePath());

        return merged;
    }

    private SummaryPayload processSingleMessage(String message) throws IOException {
        JsonNode root = objectMapper.readTree(message);

        String jobName = safeGetText(root, "JobName", false);
        if (jobName == null) jobName = "";

        String batchId = safeGetText(root, "BatchId", true);
        if (batchId == null) batchId = UUID.randomUUID().toString();

        // Process customer summaries
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

        // Build header info from JSON
        HeaderInfo headerInfo = buildHeader(root, jobName);
        if (headerInfo.getBatchId() == null) {
            headerInfo.setBatchId(batchId);
        }

        // Build payload info from JSON
        PayloadInfo payloadInfo = new PayloadInfo();
        payloadInfo.setUniqueConsumerRef(safeGetText(root, "uniqueConsumerRef", false));
        payloadInfo.setUniqueECPBatchRef(safeGetText(root, "uniqueECPBatchRef", false));
        payloadInfo.setRunPriority(safeGetText(root, "runPriority", false));
        payloadInfo.setEventID(safeGetText(root, "eventID", false));
        payloadInfo.setEventType(safeGetText(root, "eventType", false));
        payloadInfo.setRestartKey(safeGetText(root, "restartKey", false));
        payloadInfo.setPrintFiles(Collections.emptyList());  // Adjust if you have printFiles in input

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setMetadata(metaDataInfo);
        summaryPayload.setSummaryURL(summaryFile.getAbsolutePath());

        return summaryPayload;
    }

    private HeaderInfo buildHeader(JsonNode root, String jobName) {
        HeaderInfo headerInfo = new HeaderInfo();
        headerInfo.setBatchId(safeGetText(root, "BatchId", false));
        headerInfo.setTenantCode(safeGetText(root, "TenantCode", false));
        headerInfo.setChannelID(safeGetText(root, "ChannelID", false));
        headerInfo.setAudienceID(safeGetText(root, "AudienceID", false));
        headerInfo.setTimestamp(safeGetText(root, "Timestamp", false));
        headerInfo.setSourceSystem(safeGetText(root, "SourceSystem", false));
        headerInfo.setProduct(safeGetText(root, "Product", false));
        headerInfo.setJobName(jobName);
        return headerInfo;
    }

    private boolean isEncrypted(String filePath, String extension) {
        // Placeholder encryption logic
        return "pdf".equalsIgnoreCase(extension);
    }

    private String determineType(String filePath) {
        // Placeholder file type determination logic
        if (filePath.toLowerCase().endsWith(".pdf")) {
            return "PDF";
        } else if (filePath.toLowerCase().endsWith(".txt")) {
            return "TEXT";
        }
        return "UNKNOWN";
    }

    private String getFileExtension(String fileName) {
        int lastDotIndex = fileName.lastIndexOf('.');
        if (lastDotIndex > 0 && lastDotIndex < fileName.length() - 1) {
            return fileName.substring(lastDotIndex + 1);
        }
        return "";
    }

    private String safeGetText(JsonNode node, String fieldName, boolean required) {
        if (node != null && node.has(fieldName) && !node.get(fieldName).isNull()) {
            return node.get(fieldName).asText();
        }
        if (required) {
            logger.warn("Required field '{}' missing in JSON", fieldName);
        }
        return null;
    }

    private Map<String, Object> generateErrorResponse(String code, String message) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("status", code);
        error.put("message", message);
        return error;
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

