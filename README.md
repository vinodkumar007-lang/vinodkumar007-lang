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
        header.setTenantCode(safeGetText(node, "tenantCode", false));
        header.setChannelID(safeGetText(node, "channelID", false));
        header.setAudienceID(safeGetText(node, "audienceID", false));

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
        Map<String, Object> responseMap = new LinkedHashMap<>();
        responseMap.put("message", "Batch processed successfully");
        responseMap.put("status", "success");

        Map<String, Object> summaryPayload = new LinkedHashMap<>();

        summaryPayload.put("batchID", finalSummary.getHeader().getBatchId());

        Map<String, Object> header = new LinkedHashMap<>();
        header.put("tenantCode", finalSummary.getHeader().getTenantCode());
        header.put("channelID", finalSummary.getHeader().getChannelID());
        header.put("audienceID", finalSummary.getHeader().getAudienceID());
        header.put("timestamp", new Date().toString());
        header.put("sourceSystem", finalSummary.getHeader().getSourceSystem() != null ? finalSummary.getHeader().getSourceSystem() : "DEBTMAN");
        header.put("product", null); // Adjust if needed
        header.put("jobName", finalSummary.getHeader().getJobName());

        summaryPayload.put("header", header);

        Map<String, Object> metadata = new LinkedHashMap<>();
        List<CustomerSummary> customers = finalSummary.getMetaData().getCustomerSummaries();
        metadata.put("totalFilesProcessed", customers.stream().mapToInt(cs -> cs.getFiles().size()).sum());
        metadata.put("processingStatus", finalSummary.getHeader().getBatchStatus());
        metadata.put("eventOutcomeCode", null); // Populate if available
        metadata.put("eventOutcomeDescription", null); // Populate if available

        summaryPayload.put("metadata", metadata);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("uniqueConsumerRef", finalSummary.getPayload().getUniqueConsumerRef());
        payload.put("uniqueECPBatchRef", finalSummary.getPayload().getUniqueECPBatchRef());
        payload.put("runPriority", finalSummary.getPayload().getRunPriority());
        payload.put("eventID", finalSummary.getPayload().getEventID());
        payload.put("eventType", finalSummary.getPayload().getEventType());
        payload.put("restartKey", finalSummary.getPayload().getRestartKey());
        summaryPayload.put("payload", payload);
        summaryPayload.put("summaryFileURL", summaryFile.getAbsolutePath());
        summaryPayload.put("timestamp", new Date().toString());

        responseMap.put("summaryPayload", summaryPayload);

        return responseMap;
    }

    private SummaryPayload mergeSummaryPayloads(List<SummaryPayload> payloads) {
        if (payloads == null || payloads.isEmpty()) {
            return new SummaryPayload();
        }

        SummaryPayload merged = new SummaryPayload();

        HeaderInfo mergedHeader = new HeaderInfo();
        PayloadInfo mergedPayload = new PayloadInfo();
        Map<String, CustomerSummary> mergedCustomers = new HashMap<>();
        Set<String> printFileSet = new LinkedHashSet<>(); // avoid duplicates

        for (SummaryPayload p : payloads) {
            HeaderInfo h = p.getHeader();
            if (h != null) {
                if (isNonEmpty(h.getBatchId())) mergedHeader.setBatchId(h.getBatchId());
                if (isNonEmpty(h.getBatchStatus())) mergedHeader.setBatchStatus(h.getBatchStatus());
                if (isNonEmpty(h.getJobName())) mergedHeader.setJobName(h.getJobName());
                if (isNonEmpty(h.getSourceSystem())) mergedHeader.setSourceSystem(h.getSourceSystem());
                if (isNonEmpty(h.getTenantCode())) mergedHeader.setTenantCode(h.getTenantCode());
                if (isNonEmpty(h.getChannelID())) mergedHeader.setChannelID(h.getChannelID());
                if (isNonEmpty(h.getAudienceID())) mergedHeader.setAudienceID(h.getAudienceID());
            }

            PayloadInfo pl = p.getPayload();
            if (pl != null) {
                if (isNonEmpty(pl.getUniqueConsumerRef())) mergedPayload.setUniqueConsumerRef(pl.getUniqueConsumerRef());
                if (isNonEmpty(pl.getUniqueECPBatchRef())) mergedPayload.setUniqueECPBatchRef(pl.getUniqueECPBatchRef());
                if (isNonEmpty(pl.getRunPriority())) mergedPayload.setRunPriority(pl.getRunPriority());
                if (isNonEmpty(pl.getEventID())) mergedPayload.setEventID(pl.getEventID());
                if (isNonEmpty(pl.getEventType())) mergedPayload.setEventType(pl.getEventType());
                if (isNonEmpty(pl.getRestartKey())) mergedPayload.setRestartKey(pl.getRestartKey());

                if (pl.getPrintFiles() != null) {
                    printFileSet.addAll(pl.getPrintFiles());
                }
            }

            if (p.getMetaData() != null && p.getMetaData().getCustomerSummaries() != null) {
                for (CustomerSummary cs : p.getMetaData().getCustomerSummaries()) {
                    if (cs == null || cs.getCustomerId() == null) continue;
                    CustomerSummary existing = mergedCustomers.get(cs.getCustomerId());
                    if (existing == null) {
                        mergedCustomers.put(cs.getCustomerId(), cs);
                    } else {
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

        mergedPayload.setPrintFiles(new ArrayList<>(printFileSet));
        merged.setHeader(mergedHeader);
        merged.setPayload(mergedPayload);

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(new ArrayList<>(mergedCustomers.values()));
        merged.setMetaData(metaDataInfo);

        return merged;
    }
    private boolean isNonEmpty(String s) {
        return s != null && !s.trim().isEmpty();
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
package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SummaryJsonWriter {
    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void appendToSummaryJson(File summaryFile, SummaryPayload newPayload, String azureBlobStorageAccount) {
        try {
            ObjectNode root;
            if (summaryFile.exists()) {
                root = (ObjectNode) mapper.readTree(summaryFile);
            } else {
                root = mapper.createObjectNode();
                root.put("batchID", newPayload.getHeader().getBatchId());
                root.put("fileName", "DEBTMAN_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + ".csv");

                // Properly populate header block
                ObjectNode headerNode = mapper.createObjectNode();
                headerNode.put("tenantCode", newPayload.getHeader().getTenantCode());
                headerNode.put("channelID", newPayload.getHeader().getChannelID());
                headerNode.put("audienceID", newPayload.getHeader().getAudienceID());
                headerNode.put("timestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
                headerNode.put("sourceSystem", newPayload.getHeader().getSourceSystem());
                headerNode.put("product", "DEBTMANAGER");
                headerNode.put("jobName", newPayload.getHeader().getJobName());
                root.set("header", headerNode);

                root.set("processedFiles", mapper.createArrayNode());
                root.set("printFiles", mapper.createArrayNode());
            }

            // Append processedFiles
            ArrayNode processedFiles = (ArrayNode) root.withArray("processedFiles");
            for (CustomerSummary customer : newPayload.getMetaData().getCustomerSummaries()) {
                ObjectNode custNode = mapper.createObjectNode();
                custNode.put("customerID", customer.getCustomerId());
                custNode.put("accountNumber", customer.getAccountNumber());
                String acc = customer.getAccountNumber();
                String batchId = newPayload.getHeader().getBatchId();
                custNode.put("pdfArchiveFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/archive", acc, batchId, "pdf"));
                custNode.put("pdfEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/email", acc, batchId, "pdf"));
                custNode.put("htmlEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/html", acc, batchId, "html"));
                custNode.put("txtEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/txt", acc, batchId, "txt"));
                custNode.put("pdfMobstatFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", acc, batchId, "pdf"));
                custNode.put("statusCode", "OK");
                custNode.put("statusDescription", "Success");
                processedFiles.add(custNode);
            }

            // Append print files
            ArrayNode printFiles = (ArrayNode) root.withArray("printFiles");
            for (String pf : newPayload.getPayload().getPrintFiles()) {
                ObjectNode pfNode = mapper.createObjectNode();
                pfNode.put("printFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", pf, newPayload.getHeader().getBatchId(), "ps"));
                printFiles.add(pfNode);
            }

            mapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, root);
            logger.info("Appended to summary.json: {}", summaryFile.getAbsolutePath());

        } catch (IOException e) {
            logger.error("Error appending to summary.json", e);
        }
    }

    // --- Existing private method unchanged ---
    private static ObjectNode buildPayloadNode(SummaryPayload payload, String azureBlobStorageAccount) {
        ObjectNode rootNode = mapper.createObjectNode();

        // Batch ID
        rootNode.put("batchID", payload.getHeader().getBatchId());

        // File name (assume naming convention)
        String fileName = "DEBTMAN_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + ".csv";
        rootNode.put("fileName", fileName);

        // Header block
        ObjectNode headerNode = mapper.createObjectNode();
        headerNode.put("tenantCode", payload.getHeader().getTenantCode());
        headerNode.put("channelID", payload.getHeader().getChannelID());
        headerNode.put("audienceID", payload.getHeader().getAudienceID());
        headerNode.put("timestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
        headerNode.put("sourceSystem", payload.getHeader().getSourceSystem());
        headerNode.put("product", "DEBTMANAGER");
        headerNode.put("jobName", payload.getHeader().getJobName());
        rootNode.set("header", headerNode);

        // Processed files
        ArrayNode processedFiles = mapper.createArrayNode();
        for (CustomerSummary customer : payload.getMetaData().getCustomerSummaries()) {
            ObjectNode custNode = mapper.createObjectNode();
            custNode.put("customerID", customer.getCustomerId());
            custNode.put("accountNumber", customer.getAccountNumber());

            String accountId = customer.getAccountNumber();
            String batchId = payload.getHeader().getBatchId();

            // Add document URLs
            custNode.put("pdfArchiveFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/archive", accountId, batchId, "pdf"));
            custNode.put("pdfEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/email", accountId, batchId, "pdf"));
            custNode.put("htmlEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/html", accountId, batchId, "html"));
            custNode.put("txtEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/txt", accountId, batchId, "txt"));
            custNode.put("pdfMobstatFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", accountId, batchId, "pdf"));

            custNode.put("statusCode", "OK");
            custNode.put("statusDescription", "Success");

            processedFiles.add(custNode);
        }
        rootNode.set("processedFiles", processedFiles);

        // Print files
        ArrayNode printFilesNode = mapper.createArrayNode();
        List<String> printFiles = payload.getPayload().getPrintFiles();
        if (printFiles != null) {
            for (String printFileName : printFiles) {
                ObjectNode printNode = mapper.createObjectNode();
                printNode.put("printFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", printFileName, payload.getHeader().getBatchId(), "ps"));
                printFilesNode.add(printNode);
            }
        }
        rootNode.set("printFiles", printFilesNode);

        return rootNode;
    }

    // --- New method added to merge existing JSON with incoming ---
    private static ObjectNode mergeSummaryJson(ObjectNode existing, ObjectNode incoming) {
        // Merge processedFiles arrays without duplicates by customerID
        ArrayNode existingFiles = (ArrayNode) existing.get("processedFiles");
        if (existingFiles == null) {
            existingFiles = mapper.createArrayNode();
            existing.set("processedFiles", existingFiles);
        }

        ArrayNode incomingFiles = (ArrayNode) incoming.get("processedFiles");
        Set<String> existingCustomerIds = new HashSet<>();
        for (JsonNode node : existingFiles) {
            existingCustomerIds.add(node.get("customerID").asText());
        }

        if (incomingFiles != null) {
            for (JsonNode node : incomingFiles) {
                String custId = node.get("customerID").asText();
                if (!existingCustomerIds.contains(custId)) {
                    existingFiles.add(node);
                    existingCustomerIds.add(custId);
                }
            }
        }

        // Merge printFiles arrays without duplicates by URL
        ArrayNode existingPrintFiles = (ArrayNode) existing.get("printFiles");
        if (existingPrintFiles == null) {
            existingPrintFiles = mapper.createArrayNode();
            existing.set("printFiles", existingPrintFiles);
        }

        ArrayNode incomingPrintFiles = (ArrayNode) incoming.get("printFiles");
        Set<String> existingPrintFileUrls = new HashSet<>();
        for (JsonNode node : existingPrintFiles) {
            existingPrintFileUrls.add(node.get("printFileURL").asText());
        }

        if (incomingPrintFiles != null) {
            for (JsonNode node : incomingPrintFiles) {
                String url = node.get("printFileURL").asText();
                if (!existingPrintFileUrls.contains(url)) {
                    existingPrintFiles.add(node);
                    existingPrintFileUrls.add(url);
                }
            }
        }

        // Update header timestamp with latest (incoming)
        ObjectNode existingHeader = (ObjectNode) existing.get("header");
        ObjectNode incomingHeader = (ObjectNode) incoming.get("header");
        if (existingHeader != null && incomingHeader != null) {
            existingHeader.put("timestamp", incomingHeader.get("timestamp").asText());
        }

        // Optionally you can merge other header fields here

        return existing;
    }

    // --- Existing helper method unchanged ---
    private static String buildBlobUrl(String account, String path, String id, String batchId, String ext) {
        return String.format("https://%s/%s/%s_%s.%s", account, path, id, batchId, ext);
    }
}
