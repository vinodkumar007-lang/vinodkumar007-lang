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

        Set<String> archived = new HashSet<>();
        Set<String> emailed = new HashSet<>();
        Set<String> mobstat = new HashSet<>();
        Set<String> printed = new HashSet<>();

        for (String message : allMessages) {
            JsonNode root = objectMapper.readTree(message);
            if (batchId == null) {
                batchId = safeGetText(root, "consumerReference", false);
                if (batchId == null) {
                    batchId = safeGetText(root, "BatchId", false);
                }
            }

            JsonNode batchFilesNode = root.get("BatchFiles");
            if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
                logger.warn("No BatchFiles found in message: {}", message);
                continue;
            }

            for (JsonNode fileNode : batchFilesNode) {
                String filePath = safeGetText(fileNode, "fileLocation", false);
                if (filePath == null) {
                    filePath = safeGetText(fileNode, "BlobUrl", false);
                }

                String objectId = safeGetText(fileNode, "ObjectId", false);
                if (filePath == null || objectId == null) {
                    logger.warn("Missing fileLocation or ObjectId in BatchFile: {}", fileNode.toString());
                    continue;
                }

                try {
                    blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
                } catch (Exception e) {
                    logger.warn("Failed to generate SAS URL for file {}", filePath, e);
                }

                String extension = getFileExtension(filePath);
                String customerId = objectId.split("_")[0];

                if (fileName.isEmpty()) {
                    fileName = safeGetText(fileNode, "fileName", false);
                    if (fileName == null || fileName.isEmpty()) {
                        fileName = safeGetText(fileNode, "Filename", false);
                    }
                }

                if (jobName.isEmpty()) {
                    jobName = safeGetText(fileNode, "jobName", false);
                    if (jobName == null || jobName.isEmpty()) {
                        jobName = safeGetText(fileNode, "JobName", false);
                    }
                }

                CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
                detail.setObjectId(objectId);
                detail.setFileLocation(filePath);
                detail.setFileUrl("https://" + azureBlobStorageAccount + "/" + filePath);
                detail.setStatus(extension.equals(".ps") ? "failed" : "OK");
                detail.setEncrypted(isEncrypted(filePath, extension));
                detail.setType(determineType(filePath, extension));

                if (filePath.contains("mobstat")) mobstat.add(customerId);
                if (filePath.contains("archive")) archived.add(customerId);
                if (filePath.contains("email")) emailed.add(customerId);
                if (extension.equals(".ps")) printed.add(customerId);

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
        }

        List<Map<String, Object>> processedFiles = new ArrayList<>();
        for (CustomerSummary customer : customerSummaries) {
            Map<String, Object> pf = new HashMap<>();
            pf.put("customerID", customer.getCustomerId());
            pf.put("accountNumber", customer.getAccountNumber());

            for (CustomerSummary.FileDetail detail : customer.getFiles()) {
                String key = switch (detail.getType()) {
                    case "pdf_archive" -> "pdfArchiveFileURL";
                    case "pdf_email" -> "pdfEmailFileURL";
                    case "html_email" -> "htmlEmailFileURL";
                    case "txt_email" -> "txtEmailFileURL";
                    case "pdf_mobstat" -> "pdfMobstatFileURL";
                    default -> null;
                };
                if (key != null) {
                    pf.put(key, detail.getFileUrl());
                }
            }

            pf.put("statusCode", "OK");
            pf.put("statusDescription", "Success");
            processedFiles.add(pf);
        }

        List<Map<String, Object>> printFiles = new ArrayList<>();
        for (CustomerSummary customer : customerSummaries) {
            for (CustomerSummary.FileDetail detail : customer.getFiles()) {
                if ("ps_print".equals(detail.getType())) {
                    Map<String, Object> pf = new HashMap<>();
                    pf.put("printFileURL", "https://" + azureBlobStorageAccount + "/pdfs/mobstat/" + detail.getObjectId());
                    printFiles.add(pf);
                }
            }
        }

        JsonNode firstRoot = objectMapper.readTree(allMessages.get(0));
        HeaderInfo headerInfo = buildHeader(firstRoot, jobName);

        PayloadInfo payloadInfo = new PayloadInfo();
        payloadInfo.setProcessedFiles(processedFiles);
        payloadInfo.setPrintFiles(printFiles);

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setMetadata(metaDataInfo);
        return summaryPayload;
    }

    private HeaderInfo buildHeader(JsonNode root, String fallbackJobName) {
        HeaderInfo headerInfo = new HeaderInfo();
        try {
            headerInfo.setBatchId(safeGetText(root, "consumerReference", false));
            if (headerInfo.getBatchId() == null) {
                headerInfo.setBatchId(safeGetText(root, "BatchId", false));
            }

            headerInfo.setTenantCode(safeGetText(root, "tenantCode", false));
            headerInfo.setChannelID(safeGetText(root, "channelId", false));
            headerInfo.setAudienceID(safeGetText(root, "audienceId", false));
            headerInfo.setSourceSystem(safeGetText(root, "sourceSystem", false));
            headerInfo.setProduct(safeGetText(root, "product", false));

            String jobName = safeGetText(root, "jobName", false);
            if (jobName == null) {
                jobName = safeGetText(root, "JobName", false);
            }
            headerInfo.setJobName(jobName != null ? jobName : fallbackJobName);
            headerInfo.setTimestamp(new Date().toString());
        } catch (IOException e) {
            logger.warn("Failed to extract header fields", e);
        }
        return headerInfo;
    }

    private String safeGetText(JsonNode node, String fieldName, boolean required) throws IOException {
        if (node == null) return null;
        if (node.has(fieldName)) return node.get(fieldName).asText();

        String capitalized = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        if (node.has(capitalized)) return node.get(capitalized).asText();

        String upper = fieldName.toUpperCase();
        if (node.has(upper)) return node.get(upper).asText();

        if (required) {
            throw new IOException("Missing required field: " + fieldName);
        }
        return null;
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

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Batch processed successfully");
        response.put("status", "success");
        response.put("summaryPayload", summaryPayload);

        kafkaTemplate.send(outputTopic, summaryPayload.getHeader().getBatchId(), objectMapper.writeValueAsString(response));
    }

    private String getFileExtension(String path) {
        int index = path.lastIndexOf('.');
        return (index >= 0) ? path.substring(index).toLowerCase() : "";
    }

    private boolean isEncrypted(String location, String extension) {
        return extension.equals(".pdf") && location.contains("encrypted");
    }

    private String determineType(String location, String extension) {
        if (extension.equals(".pdf") && location.contains("archive")) return "pdf_archive";
        if (extension.equals(".pdf") && location.contains("email")) return "pdf_email";
        if (extension.equals(".html") && location.contains("email")) return "html_email";
        if (extension.equals(".txt") && location.contains("email")) return "txt_email";
        if (extension.equals(".pdf") && location.contains("mobstat")) return "pdf_mobstat";
        if (extension.equals(".ps")) return "ps_print";
        return "unknown";
    }

    private Map<String, Object> generateErrorResponse(String code, String message) {
        Map<String, Object> error = new HashMap<>();
        error.put("statusCode", code);
        error.put("statusMessage", message);
        return Collections.singletonMap("error", error);
    }
}
