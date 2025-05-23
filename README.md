// Full updated KafkaListenerService.java

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
            consumer.subscribe(List.of(inputTopic));
            List<String> allMessages = new ArrayList<>();
            int emptyPollCount = 0;

            while (emptyPollCount < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPollCount++;
                } else {
                    emptyPollCount = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        // Only accept messages newer than some logic if needed (e.g., last 1 day)
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

            JsonNode batchFilesNode = root.get("BatchFiles");
            if (batchFilesNode == null || !batchFilesNode.isArray()) {
                batchFilesNode = root.get("batchFiles");
            }

            if (batchFilesNode == null || !batchFilesNode.isArray()) {
                logger.warn("No BatchFiles found in message.");
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
                    logger.warn("Blob upload failed for {}", filePath, e);
                }

                String extension = getFileExtension(filePath);
                CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
                detail.setObjectId(objectId);
                detail.setFileLocation(filePath);
                detail.setFileUrl("https://" + azureBlobStorageAccount + "/" + filePath);
                detail.setEncrypted(isEncrypted(filePath, extension));
                detail.setStatus(validationStatus != null ? validationStatus : "OK");
                detail.setType(determineType(filePath, extension));

                String customerId = objectId;
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

            if (jobName == null || jobName.isEmpty()) {
                jobName = safeGetText(root, "JobName", false);
            }

            if (fileName.isEmpty() && batchFilesNode.size() > 0) {
                JsonNode firstFileNode = batchFilesNode.get(0);
                fileName = safeGetText(firstFileNode, "Filename", false);
            }
        }

        JsonNode firstMessage = objectMapper.readTree(allMessages.get(0));
        HeaderInfo headerInfo = buildHeader(firstMessage, jobName);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);

        PayloadInfo payloadInfo = new PayloadInfo();
        payloadInfo.setProcessedFiles(null); // remove from response
        payloadInfo.setPrintFiles(Collections.emptyList());
        summaryPayload.setPayload(payloadInfo);

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);
        summaryPayload.setMetadata(metaDataInfo);

        return summaryPayload;
    }

    private HeaderInfo buildHeader(JsonNode root, String jobName) {
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

    private File writeSummaryToFile(SummaryPayload summaryPayload) throws IOException {
        String userHome = System.getProperty("user.home");
        File jsonFile = new File(userHome, "summary.json");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, List.of(Map.of(
                "batchID", summaryPayload.getHeader().getBatchId(),
                "fileName", summaryPayload.getHeader().getJobName(),
                "header", summaryPayload.getHeader(),
                "printFiles", summaryPayload.getPayload().getPrintFiles()
        )));
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
