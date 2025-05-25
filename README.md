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

        // Put fields at top level as requested (you can adjust field names casing if needed)
        responseMap.put("BatchID", finalSummary.getHeader().getBatchId());
        responseMap.put("TenantCode", finalSummary.getHeader().getTenantCode());
        responseMap.put("ChannelID", finalSummary.getHeader().getChannelID());
        responseMap.put("AudienceID", finalSummary.getHeader().getAudienceID());
        responseMap.put("Timestamp", new Date().toString());
        responseMap.put("SourceSystem",
                finalSummary.getHeader().getSourceSystem() != null ? finalSummary.getHeader().getSourceSystem() : "DEBTMAN");
        responseMap.put("Product", finalSummary.getHeader().getProduct());  // Adjust as needed, no Product field in current model
        responseMap.put("JobName", finalSummary.getHeader().getJobName());

        responseMap.put("UniqueConsumerRef", finalSummary.getPayload().getUniqueConsumerRef());
        responseMap.put("BlobURL", finalSummary.getPayload().getBlobURL());  // No direct BlobURL field available; add if you have
        responseMap.put("FilenameOnBlobStorage", summaryFile.getName()); // Using summary file name as example
        responseMap.put("RunPriority", finalSummary.getPayload().getRunPriority());
        responseMap.put("EventType", finalSummary.getPayload().getEventType());
        responseMap.put("RestartKey", finalSummary.getPayload().getRestartKey());

        // Calculated fields
        int totalFilesProcessed = 0;
        List<CustomerSummary> customers = finalSummary.getMetaData() != null
                ? finalSummary.getMetaData().getCustomerSummaries() : Collections.emptyList();
        for (CustomerSummary c : customers) {
            if (c.getFiles() != null) {
                totalFilesProcessed += c.getFiles().size();
            }
        }
        responseMap.put("TotalFilesProcessed", totalFilesProcessed);

        responseMap.put("ProcessingStatus", finalSummary.getHeader().getBatchStatus());

        // No source for these in current code, so left null placeholders
        responseMap.put("EventOutcomeCode", finalSummary.getHeader().getEventOutcomeCode());
        responseMap.put("EventOutcomeDescription", finalSummary.getHeader().getEventOutcomeDescription());

        // Summary report file location as absolute path of summary file
        responseMap.put("SummaryReportFileLocation", summaryFile.getAbsolutePath());

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
