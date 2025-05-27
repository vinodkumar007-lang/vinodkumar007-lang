package com.nedbank.kafka.filemanage.service;

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
    private final Map<TopicPartition, Long> lastProcessedOffsets = new HashMap<>();

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public Map<String, Object> listen() {
        logger.info("Starting to listen for Kafka messages...");
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        List<SummaryPayload> processedPayloads = new ArrayList<>();
        Map<TopicPartition, Long> newOffsets = new HashMap<>();

        try {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(inputTopic).forEach(p -> partitions.add(new TopicPartition(p.topic(), p.partition())));
            consumer.assign(partitions);
            logger.info("Assigned to partitions: {}", partitions);

            for (TopicPartition partition : partitions) {
                if (lastProcessedOffsets.containsKey(partition)) {
                    long nextOffset = lastProcessedOffsets.get(partition) + 1;
                    consumer.seek(partition, nextOffset);
                    logger.info("Seeking partition {} to offset {}", partition.partition(), nextOffset);
                } else {
                    consumer.seekToBeginning(Collections.singletonList(partition));
                    logger.info("Seeking partition {} to beginning", partition.partition());
                }
            }

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            logger.info("Polled {} record(s)", records.count());

            for (ConsumerRecord<String, String> record : records) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());

                if (lastProcessedOffsets.containsKey(tp) && record.offset() <= lastProcessedOffsets.get(tp)) {
                    logger.debug("Skipping already processed record at offset {}", record.offset());
                    continue;
                }

                logger.info("Processing message at offset {} from partition {}", record.offset(), record.partition());
                SummaryPayload payload = processSingleMessage(record.value());

                if (payload.getBatchId() == null || payload.getBatchId().isBlank()) {
                    logger.warn("Skipping message with missing batch ID at offset {}", record.offset());
                    continue;
                }

                payload.setTopic(record.topic());
                payload.setPartition(record.partition());
                payload.setOffset(record.offset());

                processedPayloads.add(payload);
                newOffsets.put(tp, record.offset());
            }

            if (processedPayloads.isEmpty()) {
                logger.warn("No new valid messages to process.");
                return generateErrorResponse("204", "No new valid messages to process.");
            }

            logger.info("Appending payloads to summary.json file...");
            SummaryJsonWriter.appendToSummaryJson(summaryFile, processedPayloads, azureBlobStorageAccount);

            logger.info("Uploading summary.json to blob storage...");
            String summaryFileUrl = blobStorageService.uploadSummaryJson(summaryFile);
            logger.info("Summary file uploaded to: {}", summaryFileUrl);

            for (SummaryPayload payload : processedPayloads) {
                payload.setSummaryFileURL(summaryFileUrl);
                kafkaTemplate.send(outputTopic, payload.getBatchId(), objectMapper.writeValueAsString(payload));
                logger.info("Sent processed payload to Kafka topic: {}", outputTopic);
            }

            lastProcessedOffsets.putAll(newOffsets);

            return buildBatchFinalResponse(processedPayloads);

        } catch (Exception e) {
            logger.error("Error processing Kafka messages", e);
            return generateErrorResponse("500", "Internal Server Error.");
        } finally {
            consumer.close();
            logger.info("Kafka consumer closed.");
        }
    }

    private Map<String, Object> buildBatchFinalResponse(List<SummaryPayload> payloads) {
        Map<String, Object> batch = new LinkedHashMap<>();
        batch.put("messagesProcessed", payloads.size());

        List<Map<String, Object>> individual = new ArrayList<>();
        for (SummaryPayload payload : payloads) {
            individual.add(buildFinalResponse(payload));
        }
        batch.put("payloads", individual);
        return batch;
    }

    private Map<String, Object> buildFinalResponse(SummaryPayload payload) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("message", "Processed successfully");
        response.put("status", "SUCCESS");
        response.put("summaryPayload", payload);
        return response;
    }

    private Map<String, Object> generateErrorResponse(String status, String message) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("status", status);
        error.put("message", message);
        return error;
    }

    private SummaryPayload processSingleMessage(String message) throws IOException {
        logger.debug("Parsing message JSON");
        JsonNode root = objectMapper.readTree(message);
        JsonNode payloadNode = root.get("Payload");

        String sourceSystem = safeGetText(root, "sourceSystem", false);
        if (sourceSystem == null && payloadNode != null) {
            sourceSystem = safeGetText(payloadNode, "sourceSystem", false);
        }
        if (sourceSystem == null) sourceSystem = "DEBTMAN";

        String jobName = safeGetText(root, "JobName", false);
        String batchId = safeGetText(root, "BatchId", true);
        if (batchId == null) batchId = UUID.randomUUID().toString();
        String timestamp = Instant.now().toString();

        String consumerReference = safeGetText(root, "consumerReference", false);
        if (consumerReference == null && payloadNode != null) {
            consumerReference = safeGetText(payloadNode, "consumerReference", false);
        }
        if (consumerReference == null) consumerReference = "unknownConsumer";

        String processReference = safeGetText(root, "eventID", false);
        if (processReference == null && payloadNode != null) {
            processReference = safeGetText(payloadNode, "eventID", false);
        }
        if (processReference == null) processReference = "unknownProcess";

        List<CustomerSummary> customerSummaries = new ArrayList<>();
        JsonNode batchFiles = root.get("BatchFiles");
        if (batchFiles != null && batchFiles.isArray()) {
            for (JsonNode file : batchFiles) {
                String blobUrl = safeGetText(file, "BlobUrl", true);
                String objectId = safeGetText(file, "ObjectId", true);
                String status = safeGetText(file, "ValidationStatus", false);

                if (blobUrl == null || objectId == null) continue;

                logger.info("Copying file from blob URL to storage: {}", blobUrl);
                String newBlobUrl = blobStorageService.uploadFileAndReturnLocation(
                        sourceSystem, blobUrl, batchId, objectId,
                        consumerReference, processReference, timestamp
                );
                logger.info("Copied file to: {}", newBlobUrl);

                CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
                detail.setObjectId(objectId);
                detail.setFileLocation(blobUrl);
                detail.setFileUrl(newBlobUrl);
                detail.setEncrypted(blobUrl.endsWith(".enc"));
                detail.setStatus(status != null ? status : "OK");
                detail.setType(determineType(blobUrl));

                CustomerSummary customer = customerSummaries.stream()
                        .filter(c -> c.getCustomerId().equals(objectId))
                        .findFirst()
                        .orElseGet(() -> {
                            CustomerSummary c = new CustomerSummary();
                            c.setCustomerId(objectId);
                            c.setFiles(new ArrayList<>());
                            customerSummaries.add(c);
                            return c;
                        });

                customer.getFiles().add(detail);
            }
        }

        HeaderInfo header = buildHeader(root.get("Header") != null ? root.get("Header") : root, jobName);
        if (header.getBatchId() == null) header.setBatchId(batchId);
        header.setProduct(safeGetText(root, "product", false));

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

            JsonNode printFiles = payloadNode.get("printFiles");
            if (printFiles != null && printFiles.isArray()) {
                List<String> pfList = new ArrayList<>();
                for (JsonNode pf : printFiles) {
                    pfList.add(pf.asText());
                }
                payloadInfo.setPrintFiles(pfList);
            }
        }

        MetaDataInfo meta = new MetaDataInfo();
        meta.setTotalCustomers(customerSummaries.size());
        meta.setTotalFiles(customerSummaries.stream().mapToInt(c -> c.getFiles().size()).sum());

        SummaryPayload payload = new SummaryPayload();
        payload.setJobName(jobName);
        payload.setBatchId(batchId);
        payload.setCustomerSummary(customerSummaries);
        payload.setHeader(header);
        payload.setPayload(payloadInfo);
        payload.setMetaData(meta);

        logger.debug("Finished building SummaryPayload");
        return payload;
    }

    private HeaderInfo buildHeader(JsonNode node, String jobName) {
        HeaderInfo header = new HeaderInfo();
        header.setBatchId(safeGetText(node, "BatchId", false));
        header.setRunPriority(safeGetText(node, "RunPriority", false));
        header.setEventID(safeGetText(node, "EventID", false));
        header.setEventType(safeGetText(node, "EventType", false));
        header.setRestartKey(safeGetText(node, "RestartKey", false));
        header.setJobName(jobName);
        return header;
    }

    private String determineType(String path) {
        if (path.endsWith(".pdf")) return "PDF";
        if (path.endsWith(".xml")) return "XML";
        return "UNKNOWN";
    }

    private String safeGetText(JsonNode node, String field, boolean mandatory) {
        if (node != null && node.has(field)) {
            return node.get(field).asText();
        }
        if (mandatory) {
            logger.warn("Missing mandatory field: {}", field);
        }
        return null;
    }
}
