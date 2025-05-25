// [Full Updated KafkaListenerService.java]

package com.nedbank.kafka.filemanage.service;

// (Imports remain unchanged)
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.kafka.clients.consumer.*;
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
            consumer.partitionsFor(inputTopic).forEach(p -> partitions.add(new TopicPartition(p.topic(), p.partition())));
            consumer.assign(partitions);

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

        String jobName = safeGetText(root, "JobName", false);
        String batchId = safeGetText(root, "BatchId", true);
        if (batchId == null) batchId = UUID.randomUUID().toString();

        List<CustomerSummary> customerSummaries = new ArrayList<>();
        String firstBlobUrl = null;

        JsonNode batchFilesNode = root.get("BatchFiles");
        if (batchFilesNode != null && batchFilesNode.isArray()) {
            for (JsonNode fileNode : batchFilesNode) {
                String filePath = safeGetText(fileNode, "BlobUrl", true);
                String objectId = safeGetText(fileNode, "ObjectId", true);
                String validationStatus = safeGetText(fileNode, "ValidationStatus", false);

                if (filePath == null || objectId == null) continue;

                if (firstBlobUrl == null) {
                    firstBlobUrl = filePath;
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

        HeaderInfo headerInfo = buildHeader(root.get("Header") != null ? root.get("Header") : root, jobName);
        if (headerInfo.getBatchId() == null) headerInfo.setBatchId(batchId);

        headerInfo.setProduct(safeGetText(root, "Product", false));
        headerInfo.setEventOutcomeCode(safeGetText(root, "EventOutcomeCode", false));
        headerInfo.setEventOutcomeDescription(safeGetText(root, "EventOutcomeDescription", false));

        JsonNode payloadNode = root.get("Payload");
        PayloadInfo payloadInfo = new PayloadInfo();
        JsonNode source = payloadNode != null ? payloadNode : root;

        payloadInfo.setUniqueConsumerRef(safeGetText(source, "uniqueConsumerRef", false));
        payloadInfo.setUniqueECPBatchRef(safeGetText(source, "uniqueECPBatchRef", false));
        payloadInfo.setRunPriority(safeGetText(source, "runPriority", false));
        payloadInfo.setEventID(safeGetText(source, "eventID", false));
        payloadInfo.setEventType(safeGetText(source, "eventType", false));
        payloadInfo.setRestartKey(safeGetText(source, "restartKey", false));
        payloadInfo.setBlobURL(firstBlobUrl);

        JsonNode printFilesNode = source.get("printFiles");
        if (printFilesNode != null && printFilesNode.isArray()) {
            List<String> printFiles = new ArrayList<>();
            for (JsonNode pf : printFilesNode) printFiles.add(pf.asText());
            payloadInfo.setPrintFiles(printFiles);
        } else {
            payloadInfo.setPrintFiles(Collections.emptyList());
        }

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setMetaData(metaDataInfo);

        return summaryPayload;
    }

    // All other helper methods: buildHeader, isEncrypted, getFileExtension, determineType,
    // safeGetText, generateErrorResponse, isNonEmpty, mergeSummaryPayloads remain unchanged

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

    private String getFileExtension(String filePath) {
        if (filePath == null || !filePath.contains(".")) return "";
        return filePath.substring(filePath.lastIndexOf('.') + 1);
    }

    private boolean isEncrypted(String filePath, String extension) {
        return extension.equalsIgnoreCase("enc");
    }

    private String determineType(String filePath) {
        if (filePath == null) return "";
        String ext = getFileExtension(filePath).toLowerCase();
        return switch (ext) {
            case "pdf" -> "PDF";
            case "doc", "docx" -> "DOC";
            case "enc" -> "Encrypted";
            default -> "Unknown";
        };
    }

    private String safeGetText(JsonNode node, String fieldName, boolean required) {
        if (node == null || !node.has(fieldName)) {
            if (required) logger.warn("Required field '{}' missing", fieldName);
            return null;
        }
        JsonNode valueNode = node.get(fieldName);
        if (valueNode == null || valueNode.isNull()) {
            if (required) logger.warn("Field '{}' is null", fieldName);
            return null;
        }
        return valueNode.asText();
    }

    private Map<String, Object> generateErrorResponse(String code, String message) {
        Map<String, Object> errResp = new HashMap<>();
        errResp.put("code", code);
        errResp.put("message", message);
        return errResp;
    }
