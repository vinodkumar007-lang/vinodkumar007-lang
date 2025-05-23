package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
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

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public Map<String, Object> processAllMessages() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        try {
            List<TopicPartition> partitions = consumer.partitionsFor(inputTopic).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();

            consumer.assign(partitions);
            consumer.poll(Duration.ofMillis(100));
            consumer.seekToBeginning(partitions);

            List<String> allMessages = new ArrayList<>();
            int emptyPollCount = 0;

            while (emptyPollCount < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPollCount++;
                } else {
                    emptyPollCount = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        allMessages.add(record.value());
                    }
                }
            }

            if (allMessages.isEmpty()) {
                return generateErrorResponse("204", "No content processed from Kafka");
            }

            SummaryPayload summaryPayload = processMessages(allMessages);
            File jsonFile = writeSummaryToFile(summaryPayload);
            sendFinalResponseToKafka(summaryPayload, jsonFile);

            Map<String, Object> response = new HashMap<>();
            response.put("message", "Batch processed successfully");
            response.put("status", "success");
            response.put("summaryPayload", summaryPayload);

            return response;

        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages");
        } finally {
            consumer.close();
        }
    }

    private SummaryPayload processMessages(List<String> allMessages) throws IOException {
        List<CustomerSummary> customerSummaries = new ArrayList<>();
        String fileName = "";
        String jobName = "";
        String batchId = null;

        for (String message : allMessages) {
            JsonNode root = objectMapper.readTree(message);

            if (batchId == null) {
                batchId = safeGetText(root, "BatchId", false);
            }

            JsonNode batchFilesNode = root.has("BatchFiles") ? root.get("BatchFiles") : root.get("batchFiles");
            if (batchFilesNode == null || !batchFilesNode.isArray()) {
                logger.warn("No BatchFiles found");
                continue;
            }

            for (JsonNode fileNode : batchFilesNode) {
                String filePath = safeGetText(fileNode, "BlobUrl", false);
                String objectId = safeGetText(fileNode, "ObjectId", false);
                String localFileName = safeGetText(fileNode, "Filename", false);
                String validationStatus = safeGetText(fileNode, "ValidationStatus", false);

                if (filePath == null || objectId == null) continue;

                try {
                    blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
                } catch (Exception e) {
                    logger.warn("Blob URL creation failed for {}", filePath, e);
                }

                String extension = getFileExtension(filePath);
                String customerId = objectId;

                CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
                detail.setObjectId(objectId);
                detail.setFileLocation(filePath);
                detail.setFileUrl("https://" + azureBlobStorageAccount + "/" + filePath);
                detail.setEncrypted(isEncrypted(filePath, extension));
                detail.setStatus(validationStatus != null ? validationStatus : "OK");
                detail.setType(determineType(filePath, extension));

                CustomerSummary customer = customerSummaries.stream()
                        .filter(c -> c.getCustomerId().equals(customerId))
                        .findFirst()
                        .orElseGet(() -> {
                            CustomerSummary c = new CustomerSummary();
                            c.setCustomerId(customerId);
                            c.setAccountNumber("");
                            c.setFiles(new ArrayList<>());
                            customerSummaries.add(c);
                            return c;
                        });

                customer.getFiles().add(detail);
            }

            if ((jobName == null || jobName.isEmpty())) {
                String jName = safeGetText(root, "JobName", false);
                if (jName != null && !jName.isEmpty()) {
                    jobName = jName;
                }
            }

            if ((fileName == null || fileName.isEmpty()) && batchFilesNode.size() > 0) {
                JsonNode firstFileNode = batchFilesNode.get(0);
                fileName = safeGetText(firstFileNode, "Filename", false);
            }
        }

        HeaderInfo headerInfo = buildHeader(allMessages.get(0), jobName);

        List<Map<String, Object>> processedFiles = new ArrayList<>();
        for (CustomerSummary customer : customerSummaries) {
            Map<String, Object> pf = new HashMap<>();
            pf.put("customerID", customer.getCustomerId());
            pf.put("accountNumber", customer.getAccountNumber());
            for (CustomerSummary.FileDetail detail : customer.getFiles()) {
                pf.put(detail.getType() + "FileURL", detail.getFileUrl());
            }
            pf.put("statusCode", "OK");
            pf.put("statusDescription", "Success");
            processedFiles.add(pf);
        }

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);

        PayloadInfo payloadInfo = new PayloadInfo();
        payloadInfo.setProcessedFiles(processedFiles);
        payloadInfo.setPrintFiles(Collections.emptyList());
        summaryPayload.setPayload(payloadInfo);

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);
        summaryPayload.setMetadata(metaDataInfo);

        return summaryPayload;
    }

    private File writeSummaryToFile(SummaryPayload summaryPayload) throws IOException {
        String userHome = System.getProperty("user.home");
        File jsonFile = new File(userHome, "summary.json");

        Map<String, Object> summaryData = new HashMap<>();
        summaryData.put("batchID", summaryPayload.getHeader().getBatchId());
        summaryData.put("fileName", summaryPayload.getHeader().getJobName());
        summaryData.put("header", summaryPayload.getHeader());
        summaryData.put("processedFiles", summaryPayload.getPayload().getProcessedFiles());
        summaryData.put("printFiles", summaryPayload.getPayload().getPrintFiles());

        List<Map<String, Object>> summaries;
        if (jsonFile.exists()) {
            try {
                summaries = objectMapper.readValue(jsonFile, List.class);
            } catch (Exception e) {
                summaries = new ArrayList<>();
            }
        } else {
            summaries = new ArrayList<>();
        }

        summaries.add(summaryData);
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, summaries);

        summaryPayload.setSummaryFileURL(jsonFile.getAbsolutePath());
        summaryPayload.getMetadata().setSummaryFileURL(jsonFile.getAbsolutePath());

        return jsonFile;
    }

    private void sendFinalResponseToKafka(SummaryPayload summaryPayload, File jsonFile) throws JsonProcessingException {
        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", summaryPayload.getHeader().getJobName());
        kafkaMsg.put("jobName", summaryPayload.getHeader().getJobName());
        kafkaMsg.put("batchId", summaryPayload.getHeader().getBatchId());
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("summaryFileURL", jsonFile.getAbsolutePath());

        kafkaTemplate.send(outputTopic, summaryPayload.getHeader().getBatchId(), objectMapper.writeValueAsString(kafkaMsg));
        kafkaTemplate.send(outputTopic, summaryPayload.getHeader().getBatchId(), objectMapper.writeValueAsString(Map.of(
"message", "Batch processed successfully",
"status", "success",
"summaryPayload", summaryPayload
)));
}

private HeaderInfo buildHeader(String messageJson, String jobName) throws JsonProcessingException {
    JsonNode root = objectMapper.readTree(messageJson);
    HeaderInfo headerInfo = new HeaderInfo();
    headerInfo.setBatchId(safeGetText(root, "BatchId", false));
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
        String value = node.get(fieldName).asText();
        return value.equalsIgnoreCase("null") ? "" : value;
    }
    return required ? "" : null;
}

private boolean isEncrypted(String filePath, String extension) {
    return extension.equals("pdf") && filePath.contains("ENCRYPTED");
}

private String determineType(String filePath, String extension) {
    return filePath.toUpperCase().contains("PRINT") ? "print" : "processed";
}

private String getFileExtension(String filePath) {
    int dotIndex = filePath.lastIndexOf(".");
    return dotIndex >= 0 ? filePath.substring(dotIndex + 1).toLowerCase() : "";
}

private Map<String, Object> generateErrorResponse(String code, String message) {
    Map<String, Object> errorResponse = new HashMap<>();
    errorResponse.put("status", "error");
    errorResponse.put("code", code);
    errorResponse.put("message", message);
    return errorResponse;
}
}
