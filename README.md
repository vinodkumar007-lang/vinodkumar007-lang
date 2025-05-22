	
Field
	
Description
	
Type
	
Kafka Topic
	
Remarks

 	
BatchID (golden thread)
	
Unique identifier for the batch.
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
TenantCode (golden thread)
	
Tenant identifier (e.g., ZANBL).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publisg message

 	
ChannelID (golden thread)
	
Channel ID (e.g., 100 = consumer).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message


not in use yet
	
AudienceID (golden thread)
	
GUID for audience, often for security.
	
GUID
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
Timestamp
	
Time the event occurred.
	
DateTime
	
Both
	
Kafka generated

 	
SourceSystem (golden thread)
	
Source system of origin (e.g., CARD).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
Product (golden thread)
	
Product associated with the batch (e.g., CASA).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
JobName (golden thread)
	
Job identifier (e.g., SMM815).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
UniqueConsumerRef (golden thread)
	
GUID for identifying the consumer.
	
GUID
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
Blob URL (storage name where input data is)
	
GUID for identifying the ECP batch.
	
GUID
	
str-ecp-batch-composition
	
ECP process will populate on str-ecp-batch-composition

 	
Filename on blob storage 
	
 
	
 
	
str-ecp-batch-composition
	
ECP process will populate on str-ecp-batch-composition


not in use yet
	
RunPriority
	
Batch priority (High, Medium, Low).
	
String
	
str-ecp-batch-composition
	
ECP process will populate on str-ecp-batch-composition, use in publish message


not in use yet
	
EventType
	
Type of event (e.g., Completion, Restart).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message


not in use yet, rerun/reprocess to be defined
	
RestartKey
	
Key used to support restart events.
	
String
	
str-ecp-batch-composition
	
First field customer

 	
TotalFilesProcessed
	
Number of successfully processed files.
	
Integer
	
str-ecp-batch-composition-complete
	
OT team

 	
ProcessingStatus
	
Overall status (Success, Failure, Partial).
	
String
	
str-ecp-batch-composition-complete
	
OT team


what are the valid values? Eg. RC = 00. To be defined during testing
	
EventOutcomeCode
	
Code indicating result (Success, Tech Error, etc.).
	
String
	
str-ecp-batch-composition-complete
	
OT team

 	
EventOutcomeDescription
	
Human-readable explanation of the outcome.
	
String
	
str-ecp-batch-composition-complete
	
OT team

 	
SummaryReportFileLocation
	
URL or path of the summary report file.
	
URL
	
str-ecp-batch-composition-complete
	
OT team
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

// Same package declaration and imports as before

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

        SummaryFileInfo summary = new SummaryFileInfo();
        summary.setFileName(fileName);
        summary.setJobName(jobName);
        summary.setBatchId(batchId);
        summary.setTimestamp(new Date().toString());
        summary.setCustomers(customerSummaries);
        summary.setSummaryFileURL(sasUrl);

        String userHome = System.getProperty("user.home");
        File jsonFile = new File(userHome, "summary.json");

        try {
            objectMapper.writeValue(jsonFile, summary);
        } catch (IOException e) {
            return generateErrorResponse("601", "Failed to write summary file");
        }

        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", fileName);
        kafkaMsg.put("jobName", jobName);
        kafkaMsg.put("batchId", batchId);
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("pdfFileURL", sasUrl);
        kafkaTemplate.send(outputTopic, batchId, objectMapper.writeValueAsString(kafkaMsg));

        // ✅ Construct enriched response using defined POJOs
        HeaderInfo headerInfo = new HeaderInfo();

        MetaDataInfo metaData = new MetaDataInfo();


        PayloadInfo payloadInfo = new PayloadInfo();


        ProcessedFileInfo processedFileInfo = new ProcessedFileInfo();
        processedFileInfo.setProcessedFiles(customerSummaries);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setMetadata(metaData);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setProcessedFileInfo(processedFileInfo);

        return Map.of("summaryPayload", summaryPayload);
    }

    // helper methods: unchanged

    private boolean isEncrypted(String path, String ext) {
        return (ext.equals(".pdf") || ext.equals(".html") || ext.equals(".txt"))
                && (path.toLowerCase().contains("mobstat") || path.toLowerCase().contains("email"));
    }

    private String determineType(String path, String ext) {
        if (path.contains("mobstat")) return "pdf_mobstat";
        if (path.contains("archive")) return "pdf_archive";
        if (path.contains("email")) {
            if (ext.equals(".html")) return "html_email";
            if (ext.equals(".txt")) return "txt_email";
            return "pdf_email";
        }
        if (ext.equals(".ps")) return "ps_print";
        return "unknown";
    }

    private String getFileExtension(String path) {
        int dot = path.lastIndexOf('.');
        return dot > 0 ? path.substring(dot) : "";
    }

    private String extractField(JsonNode node, String name) {
        try {
            JsonNode val = node.get(name);
            return val != null ? val.asText() : null;
        } catch (Exception e) {
            logger.error("Failed to extract field {}: {}", name, e.getMessage());
            return null;
        }
    }

    private String convertPojoToJson(String raw) {
        raw = raw.trim();
        if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
            raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
        }
        raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
        raw = raw.replaceAll(":([a-zA-Z0-9_]+)", ":\"$1\"");
        return "{" + raw + "}";
    }

    private Map<String, Object> generateErrorResponse(String status, String message) {
        Map<String, Object> error = new HashMap<>();
        error.put("status", status);
        error.put("message", message);
        return error;
    }
}

