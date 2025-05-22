package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.ProcessedFileInfo;
import com.nedbank.kafka.filemanage.model.SummaryFileInfo;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import com.nedbank.kafka.filemanage.model.MetaDataInfo;
import com.nedbank.kafka.filemanage.model.PayloadInfo;
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

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public Map<String, Object> processAllMessages() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        consumer.assign(Collections.singletonList(new TopicPartition(inputTopic, 0)));
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition(inputTopic, 0)));

        Map<String, Object> response = new HashMap<>();

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> result = handleMessage(record.value());
                if (result != null) return result;
            }
        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages");
        } finally {
            consumer.close();
        }

        return response;
    }

    private Map<String, Object> handleMessage(String message) throws JsonProcessingException {
        JsonNode root;

        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            message = convertPojoToJson(message);
            try {
                root = objectMapper.readTree(message);
            } catch (Exception retryEx) {
                logger.error("Failed to parse corrected JSON", retryEx);
                return generateErrorResponse("400", "Invalid JSON format");
            }
        }

        String batchId = extractField(root, "consumerReference");
        JsonNode batchFilesNode = root.get("batchFiles");

        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            return generateErrorResponse("404", "No batch files found");
        }

        JsonNode firstFile = batchFilesNode.get(0);
        String filePath = firstFile.get("fileLocation").asText();
        String objectId = firstFile.get("ObjectId").asText();

        String sasUrl;
        try {
            sasUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
        } catch (Exception e) {
            return generateErrorResponse("453", "Error generating SAS URL");
        }

        List<CustomerSummary> customerSummaries = new ArrayList<>();
        String fileName = "";
        String jobName = "";

        Set<String> archived = new HashSet<>();
        Set<String> emailed = new HashSet<>();
        Set<String> mobstat = new HashSet<>();
        Set<String> printed = new HashSet<>();

        for (JsonNode fileNode : batchFilesNode) {
            String objId = fileNode.get("ObjectId").asText();
            String location = fileNode.get("fileLocation").asText();
            String extension = getFileExtension(location).toLowerCase();
            String customerId = objId.split("_")[0];

            if (fileNode.has("fileName")) fileName = fileNode.get("fileName").asText();
            if (fileNode.has("jobName")) jobName = fileNode.get("jobName").asText();

            CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
            detail.setObjectId(objId);
            detail.setFileLocation(location);
            detail.setFileUrl("file://" + location);
            detail.setStatus(extension.equals(".ps") ? "failed" : "OK");
            detail.setEncrypted(isEncrypted(location, extension));
            detail.setType(determineType(location, extension));

            if (location.contains("mobstat")) mobstat.add(customerId);
            if (location.contains("archive")) archived.add(customerId);
            if (location.contains("email")) emailed.add(customerId);
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

        // ✅ Updated summary data structure
        Map<String, Object> summaryData = new LinkedHashMap<>();
        summaryData.put("batchID", batchId);
        summaryData.put("fileName", fileName);

        // Header
        Map<String, Object> header = new LinkedHashMap<>();
        header.put("tenantCode", extractField(root, "tenantCode"));
        header.put("channelID", extractField(root, "channelId"));
        header.put("audienceID", extractField(root, "audienceId"));
        header.put("timestamp", new Date().toInstant().toString());
        header.put("sourceSystem", extractField(root, "sourceSystem"));
        header.put("product", extractField(root, "product"));
        header.put("jobName", jobName);
        summaryData.put("header", header);

        // Processed Files
        List<Map<String, Object>> processedFiles = new ArrayList<>();
        for (CustomerSummary customer : customerSummaries) {
            Map<String, Object> pf = new LinkedHashMap<>();
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
        summaryData.put("processedFiles", processedFiles);

        // Print Files
        List<Map<String, Object>> printFiles = new ArrayList<>();
        for (CustomerSummary customer : customerSummaries) {
            for (CustomerSummary.FileDetail detail : customer.getFiles()) {
                if ("ps_print".equals(detail.getType())) {
                    Map<String, Object> pf = new LinkedHashMap<>();
                    pf.put("printFileURL", detail.getFileUrl());
                    printFiles.add(pf);
                }
            }
        }
        summaryData.put("printFiles", printFiles);

        // Write to summary.json
        String userHome = System.getProperty("user.home");
        File jsonFile = new File(userHome, "summary.json");
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, summaryData);
        } catch (IOException e) {
            return generateErrorResponse("601", "Failed to write summary file");
        }

        // Get the path of the summary.json file to include in the response
        String summaryFilePath = jsonFile.getAbsolutePath();

        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", fileName);
        kafkaMsg.put("jobName", jobName);
        kafkaMsg.put("batchId", batchId);
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("pdfFileURL", sasUrl);
        kafkaTemplate.send(outputTopic, batchId, objectMapper.writeValueAsString(kafkaMsg));

        // ✅ Enriched Response Construction Starts Here

        HeaderInfo headerInfo = new HeaderInfo();
        headerInfo.setBatchId(batchId);
        headerInfo.setTenantCode(extractField(root, "tenantCode"));
        headerInfo.setChannelId(extractField(root, "channelId"));
        headerInfo.setAudienceId(extractField(root, "audienceId"));
        headerInfo.setTimestamp(new Date().toString());
        headerInfo.setSourceSystem(extractField(root, "sourceSystem"));
        headerInfo.setProduct(extractField(root, "product"));

        PayloadInfo payloadInfo = new PayloadInfo();
        payloadInfo.setProcessedFiles(processedFiles);
        payloadInfo.setPrintFiles(printFiles);

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);
        metaDataInfo.setSummaryFileURL(summaryFilePath);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setMetaData(metaDataInfo);

        response.put("response", summaryPayload);

        return response;
    }

    private String extractField(JsonNode root, String fieldName) {
        JsonNode fieldNode = root.get(fieldName);
        return fieldNode != null ? fieldNode.asText() : null;
    }

    private String convertPojoToJson(String pojo) {
        // Some error handling logic here
        return pojo; // returning the original message for simplicity in this example
    }

    private Map<String, Object> generateErrorResponse(String code, String message) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("errorCode", code);
        errorResponse.put("errorMessage", message);
        return errorResponse;
    }

    private String getFileExtension(String location) {
        return location.substring(location.lastIndexOf("."));
    }

    private boolean isEncrypted(String location, String extension) {
        // Check if the file is encrypted based on your business logic
        return extension.equals(".pdf");
    }

    private String determineType(String location, String extension) {
        // Your logic to determine the file type
        if (location.contains("archive")) return "pdf_archive";
        if (location.contains("email")) return "pdf_email";
        return "unknown";
    }
}
