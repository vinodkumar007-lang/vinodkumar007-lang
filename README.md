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
import java.time.Instant;
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
    // New: Map to store last processed offsets
    private final Map<TopicPartition, Long> lastProcessedOffsets = new HashMap<>();
    private String fileLocation;
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;

    }

    public Map<String, Object> listen() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        List<SummaryPayload> processedPayloads = new ArrayList<>();
        Map<TopicPartition, Long> newOffsets = new HashMap<>();

        try {
            List<TopicPartition> partitions = new ArrayList<>();

            // Discover and assign partitions
            consumer.partitionsFor(inputTopic).forEach(partitionInfo -> {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            });
            consumer.assign(partitions);

            // Seek to next unprocessed offset or beginning
            for (TopicPartition partition : partitions) {
                if (lastProcessedOffsets.containsKey(partition)) {
                    long nextOffset = lastProcessedOffsets.get(partition) + 1;
                    consumer.seek(partition, nextOffset);
                    logger.info("Seeking partition {} to offset {}", partition.partition(), nextOffset);
                } else {
                    consumer.seekToBeginning(Collections.singletonList(partition));
                    logger.warn("No previous offset for partition {}; seeking to beginning", partition.partition());
                }
            }

            // Poll messages
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            logger.info("Polled {} record(s) from Kafka", records.count());

            SummaryPayload summaryPayload = null;
            for (ConsumerRecord<String, String> record : records) {
                TopicPartition currentPartition = new TopicPartition(record.topic(), record.partition());

                // Skip already processed messages
                if (lastProcessedOffsets.containsKey(currentPartition) &&
                        record.offset() <= lastProcessedOffsets.get(currentPartition)) {
                    logger.debug("Skipping already processed offset {} for partition {}", record.offset(), record.partition());
                    continue;
                }

                logger.info("Processing record from topic-partition-offset {}-{}-{}: key='{}'",
                        record.topic(), record.partition(), record.offset(), record.key());

                summaryPayload = processSingleMessage(record.value());
                if (summaryPayload.getBatchId() == null || summaryPayload.getBatchId().trim().isEmpty()) {
                    logger.warn("Missing or empty mandatory field 'BatchId' at offset {}; skipping", record.offset());
                    continue;
                }

                // Add extra tracking info for offset management (optional, if you can extend SummaryPayload)
                summaryPayload.setTopic(record.topic());
                summaryPayload.setPartition(record.partition());
                summaryPayload.setOffset(record.offset());

                processedPayloads.add(summaryPayload);

                // Keep track of highest offset per partition for committing later
                newOffsets.put(currentPartition, record.offset());
            }

            if (processedPayloads.isEmpty()) {
                return generateErrorResponse("204", "No new valid messages available in Kafka topic.");
            }

            // Append all processed payloads at once to summary.json
            //SummaryJsonWriter.appendToSummaryJson(summaryFile, processedPayloads, azureBlobStorageAccount);
            SummaryJsonWriter.appendToSummaryJson(summaryFile, summaryPayload);
            // Update last processed offsets only after successful append and sending
            lastProcessedOffsets.putAll(newOffsets);

            return buildBatchFinalResponse(processedPayloads);

        } catch (Exception e) {
            logger.error("Error while consuming Kafka message", e);
            return generateErrorResponse("500", "Internal Server Error while processing Kafka message.");
        } finally {
            consumer.close();
        }
    }


    private Map<String, Object> buildBatchFinalResponse(List<SummaryPayload> payloads) {
        List<Map<String, Object>> processedList = new ArrayList<>();

        for (SummaryPayload payload : payloads) {
            processedList.add(buildFinalResponse(payload));
        }

        Map<String, Object> batchResponse = new HashMap<>();
        batchResponse.put("messagesProcessed", processedList.size());
        batchResponse.put("payloads", processedList);

        return batchResponse;
    }

    private SummaryPayload processSingleMessage(String message) throws IOException {
        JsonNode root = objectMapper.readTree(message);
        logger.debug("Kafka message received  : " + message);
        // ✅ Extract sourceSystem dynamically from root or Payload (fallback to DEBTMAN)
        String sourceSystem = safeGetText(root, "sourceSystem", false);
        if (sourceSystem == null) {
            JsonNode payloadNode = root.get("Payload");
            if (payloadNode != null) {
                sourceSystem = safeGetText(payloadNode, "sourceSystem", false);
            }
        }
        if (sourceSystem == null || sourceSystem.isBlank()) {
            sourceSystem = "DEBTMAN";
        }

        // Extract jobName (optional)
        String jobName = safeGetText(root, "JobName", false);
        if (jobName == null) jobName = "";

        // Extract BatchId (mandatory fallback to random UUID)
        String batchId = safeGetText(root, "BatchId", true);
        if (batchId == null) batchId = UUID.randomUUID().toString();

        // Extract timestamp (use current timestamp if not in message)
        String timestamp = Instant.now().toString();

        // Extract consumerReference
        String consumerReference = safeGetText(root, "consumerReference", false);
        JsonNode payloadNode = root.get("Payload");
        if (consumerReference == null && payloadNode != null) {
            consumerReference = safeGetText(payloadNode, "consumerReference", false);
        }
        if (consumerReference == null) consumerReference = "unknownConsumer";

        // Extract processReference (we'll use eventID as fallback)
        String processReference = safeGetText(root, "eventID", false);
        if (processReference == null && payloadNode != null) {
            processReference = safeGetText(payloadNode, "eventID", false);
        }
        if (processReference == null) processReference = "unknownProcess";

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
                    fileLocation = blobStorageService.uploadFileAndReturnLocation(
                            sourceSystem,
                            filePath,
                            batchId,
                            objectId,
                            consumerReference,
                            processReference,
                            timestamp
                    );
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

        // Build Header info
        HeaderInfo headerInfo;
        JsonNode headerNode = root.get("Header");
        if (headerNode != null && !headerNode.isNull()) {
            headerInfo = buildHeader(headerNode, jobName);
        } else {
            headerInfo = buildHeader(root, jobName);
        }
        if (headerInfo.getBatchId() == null) {
            headerInfo.setBatchId(batchId);
        }

        // Extract product field if present
        String product = null;
        if (headerNode != null && headerNode.has("product")) {
            product = safeGetText(headerNode, "product", false);
        }
        if (product == null) {
            product = safeGetText(root, "product", false);
        }
        headerInfo.setProduct(product);

        // Build Payload info
        PayloadInfo payloadInfo = new PayloadInfo();
        if (payloadNode != null && !payloadNode.isNull()) {
            payloadInfo.setUniqueConsumerRef(safeGetText(payloadNode, "uniqueConsumerRef", false));
            payloadInfo.setUniqueECPBatchRef(safeGetText(payloadNode, "uniqueECPBatchRef", false));
            payloadInfo.setRunPriority(safeGetText(payloadNode, "runPriority", false));
            payloadInfo.setEventID(safeGetText(payloadNode, "eventID", false));
            payloadInfo.setEventType(safeGetText(payloadNode, "eventType", false));
            payloadInfo.setRestartKey(safeGetText(payloadNode, "restartKey", false));
            payloadInfo.setBlobURL(safeGetText(payloadNode, "blobURL", false));
            payloadInfo.setEventOutcomeCode(safeGetText(payloadNode, "eventOutcomeCode", false));
            payloadInfo.setEventOutcomeDescription(safeGetText(payloadNode, "eventOutcomeDescription", false));

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
        Map<String, Object> response = new LinkedHashMap<>();

        response.put("jobName", summaryPayload.getJobName());
        response.put("batchId", summaryPayload.getBatchId());

        // Header
        Map<String, Object> headerMap = new LinkedHashMap<>();
        HeaderInfo header = summaryPayload.getHeader();
        if (header != null) {
            headerMap.put("batchId", header.getBatchId());
            headerMap.put("runPriority", header.getRunPriority());
            headerMap.put("eventID", header.getEventID());
            headerMap.put("eventType", header.getEventType());
            headerMap.put("restartKey", header.getRestartKey());
            headerMap.put("jobName", header.getJobName());
            headerMap.put("product", header.getProduct());
        }
        response.put("header", headerMap);

        // Metadata
        Map<String, Object> metadataMap = new LinkedHashMap<>();
        MetaDataInfo meta = summaryPayload.getMetaData();
        if (meta != null) {
            metadataMap.put("totalCustomers", meta.getTotalCustomers());
            metadataMap.put("totalFiles", meta.getTotalFiles());
        }
        response.put("metaData", metadataMap);

        // Payload
        Map<String, Object> payloadMap = new LinkedHashMap<>();
        PayloadInfo payload = summaryPayload.getPayload();
        if (payload != null) {
            payloadMap.put("uniqueConsumerRef", payload.getUniqueConsumerRef());
            payloadMap.put("uniqueECPBatchRef", payload.getUniqueECPBatchRef());
            payloadMap.put("runPriority", payload.getRunPriority());
            payloadMap.put("eventID", payload.getEventID());
            payloadMap.put("eventType", payload.getEventType());
            payloadMap.put("restartKey", payload.getRestartKey());
            payloadMap.put("blobURL", payload.getBlobURL());
            payloadMap.put("eventOutcomeCode", payload.getEventOutcomeCode());
            payloadMap.put("eventOutcomeDescription", payload.getEventOutcomeDescription());
            payloadMap.put("printFiles", payload.getPrintFiles());
        }
        response.put("payload", payloadMap);

        // CustomerSummary
        List<Map<String, Object>> customerSummaries = new ArrayList<>();
        if (summaryPayload.getCustomerSummary() != null) {
            for (CustomerSummary customer : summaryPayload.getCustomerSummary()) {
                Map<String, Object> customerMap = new LinkedHashMap<>();
                customerMap.put("customerId", customer.getCustomerId());
                customerMap.put("accountNumber", customer.getAccountNumber());

                List<Map<String, Object>> filesList = new ArrayList<>();
                for (CustomerSummary.FileDetail fileDetail : customer.getFiles()) {
                    Map<String, Object> fileMap = new LinkedHashMap<>();
                    fileMap.put("objectId", fileDetail.getObjectId());
                    fileMap.put("fileLocation", fileDetail.getFileLocation());
                    fileMap.put("fileUrl", fileDetail.getFileUrl());
                    fileMap.put("encrypted", fileDetail.isEncrypted());
                    fileMap.put("status", fileDetail.getStatus());
                    fileMap.put("type", fileDetail.getType());
                    filesList.add(fileMap);
                }

                customerMap.put("files", filesList);
                customerSummaries.add(customerMap);
            }
        }
        response.put("customerSummary", customerSummaries);

        // Optional fields (if available from setTopic, setPartition, setOffset)
        response.put("kafkaTopic", summaryPayload.getTopic());
        response.put("partition", summaryPayload.getPartition());
        response.put("offset", summaryPayload.getOffset());

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
