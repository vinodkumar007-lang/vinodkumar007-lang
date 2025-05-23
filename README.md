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

            // Process all messages, build summary payload
            SummaryPayload summaryPayload = processMessages(allMessages);

            // Write summary to JSON file
            File jsonFile = writeSummaryToFile(summaryPayload);

            // Send final response to Kafka
            sendFinalResponseToKafka(summaryPayload, jsonFile);

            // Return final enriched response
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

    // --- NEW METHOD: processes all messages and aggregates into one SummaryPayload ---
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
            }

            JsonNode batchFilesNode = root.get("BatchFiles");
            if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
                logger.warn("No BatchFiles found in message: {}", message);
                continue; // skip this message instead of throwing
            }

            for (JsonNode fileNode : batchFilesNode) {
                String filePath = safeGetText(fileNode, "fileLocation", false);
                String objectId = safeGetText(fileNode, "ObjectId", false);

                if (filePath == null || objectId == null) {
                    logger.warn("Missing fileLocation or ObjectId in BatchFile: {}", fileNode.toString());
                    continue; // skip malformed file
                }

                try {
                    blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
                } catch (Exception e) {
                    logger.warn("Failed to generate SAS URL for file {}", filePath, e);
                }

                String extension = getFileExtension(filePath).toLowerCase();
                String customerId = objectId.split("_")[0];

                if (fileNode.has("fileName")) {
                    fileName = safeGetText(fileNode, "fileName", false);
                }

                if (fileNode.has("jobName")) {
                    jobName = safeGetText(fileNode, "jobName", false);
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

        HeaderInfo headerInfo = null;
        if (!allMessages.isEmpty()) {
            JsonNode firstRoot = objectMapper.readTree(allMessages.get(0));
            headerInfo = buildHeader(firstRoot, jobName);
        } else {
            headerInfo = new HeaderInfo();
        }

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

    // --- UPDATED METHOD: append each Kafka message summary to JSON ---
    private File writeSummaryToFile(SummaryPayload summaryPayload) throws IOException {
        String userHome = System.getProperty("user.home");
        File jsonFile = new File(userHome, "summary.json");

        Map<String, Object> summaryData = new HashMap<>();
        summaryData.put("batchID", summaryPayload.getHeader().getBatchId());
        summaryData.put("fileName", summaryPayload.getHeader().getJobName());
        summaryData.put("header", summaryPayload.getHeader());
        summaryData.put("processedFiles", summaryPayload.getPayload().getProcessedFiles());
        summaryData.put("printFiles", summaryPayload.getPayload().getPrintFiles());

        // If file already exists, append to an array; otherwise start a new array
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

    // --- NEW METHOD: send final enriched response to Kafka ---
    private void sendFinalResponseToKafka(SummaryPayload summaryPayload, File jsonFile) throws JsonProcessingException {
        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", summaryPayload.getHeader().getJobName());
        kafkaMsg.put("jobName", summaryPayload.getHeader().getJobName());
        kafkaMsg.put("batchId", summaryPayload.getHeader().getBatchId());
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("summaryFileURL", jsonFile.getAbsolutePath());

        // Send a simple message with summary file info
        kafkaTemplate.send(outputTopic, summaryPayload.getHeader().getBatchId(), objectMapper.writeValueAsString(kafkaMsg));

        // Send the detailed enriched summaryPayload as well
        Map<String, Object> response = new HashMap<>();
        response.put("message", "Batch processed successfully");
        response.put("status", "success");
        response.put("summaryPayload", summaryPayload);

        kafkaTemplate.send(outputTopic, summaryPayload.getHeader().getBatchId(), objectMapper.writeValueAsString(response));
    }

    // === EXISTING METHODS (unchanged) ===

    private HeaderInfo buildHeader(JsonNode root, String jobName) {
        HeaderInfo headerInfo = new HeaderInfo();
        headerInfo.setBatchId(extractField(root, "consumerReference"));
        headerInfo.setTenantCode(extractField(root, "tenantCode"));
        headerInfo.setChannelID(extractField(root, "channelId"));
        headerInfo.setAudienceID(extractField(root, "audienceId"));
        headerInfo.setTimestamp(new Date().toString());
        headerInfo.setSourceSystem(extractField(root, "sourceSystem"));
        headerInfo.setProduct(extractField(root, "product"));
        headerInfo.setJobName(jobName);
        return headerInfo;
    }

    private String extractField(JsonNode root, String fieldName) {
        JsonNode fieldNode = root.get(fieldName);
        return fieldNode != null ? fieldNode.asText() : null;
    }

    private String convertPojoToJson(String pojo) {
        return pojo;
    }

    private Map<String, Object> generateErrorResponse(String code, String message) {
        Map<String, Object> error = new HashMap<>();
        error.put("statusCode", code);
        error.put("statusMessage", message);
        return Collections.singletonMap("error", error);
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

    private String getFileExtension(String path) {
        int index = path.lastIndexOf('.');
        return (index >= 0) ? path.substring(index).toLowerCase() : "";
    }

    private String safeGetText(JsonNode node, String fieldName, boolean required) throws IOException {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null) {
            if (required) {
                throw new IOException("Missing required field: " + fieldName);
            } else {
                return null;
            }
        }
        return fieldNode.asText();
    }

}
{
  "sourceSystem" : "DEBTMAN",
  "timestamp" : 1.747738551718507E9,
  "batchFiles" : [ {
    "fileLocation" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH"
  } ],
  "consumerReference" : "12345",
  "processReference" : "Test12345",
  "batchControlFileData" : null
}
New message [Key=null, Offset=18499]:
{
  "sourceSystem" : "DEBTMAN",
  "timestamp" : 1.74773858562743E9,
  "batchFiles" : [ {
    "fileLocation" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH"
  } ],
  "consumerReference" : "12345",
  "processReference" : "Test12345",
  "batchControlFileData" : null
}
New message [Key=null, Offset=18500]:
{
  "BatchId" : "eba7d148-19b1-49dc-8422-111896b2d184",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : null,
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : null,
  "UniqueConsumerRef" : "12345",
  "Timestamp" : 1.7479141606266396E9,
  "ProcessReference" : null,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv"
  } ]
}
New message [Key=null, Offset=18501]:
{
  "BatchId" : "bea3e8b1-cbf6-4f55-99ee-fb30dd01aa4b",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : null,
  "UniqueConsumerRef" : "12345",
  "Timestamp" : 1.7479146286245925E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv"
  } ]
}
New message [Key=null, Offset=18502]:
{
  "BatchId" : "e996ae1e-6161-4990-901c-c435def61361",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : null,
  "UniqueConsumerRef" : "29e2c73e-2031-4bd2-96fb-aa98f6f313fa",
  "Timestamp" : 1.7479148360337114E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv"
  } ]
}
New message [Key=null, Offset=18503]:
{
  "BatchId" : "6eaca62d-7ede-4109-b7cf-fddd19f53a23",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : null,
  "UniqueConsumerRef" : "c4a96e29-d7db-44f8-91bb-4c4727116595",
  "Timestamp" : 1.7479152865408785E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv"
  } ]
}
New message [Key=null, Offset=18504]:
{
  "BatchId" : "e4c2c1d3-0406-42cb-aae4-6eedc9fab3d6",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : null,
  "UniqueConsumerRef" : "6d0991ee-06c2-475f-94d2-79969a6167c4",
  "Timestamp" : 1.7479154258868415E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv"
  } ]
}
New message [Key=null, Offset=18505]:
{
  "BatchId" : "98e10e99-30fc-426c-b3fb-f7b516cb6773",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : null,
  "UniqueConsumerRef" : "d4d494b8-661a-43ae-9f44-7b929e93bc8e",
  "Timestamp" : 1.7479154523627496E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv"
  } ]
}
New message [Key=null, Offset=18506]:
{
  "BatchId" : "db31e2a1-6eb7-4514-8dbe-72da4c3b13bc",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : null,
  "UniqueConsumerRef" : "6c77a81f-e195-49b6-82ed-5e873c74924a",
  "Timestamp" : 1.7479158330833814E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18507]:
{
  "BatchId" : "7a47a018-7dc1-4559-a5f1-113427aadf1b",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "55baeb64-db5e-4fc6-a9d7-5ef412a7b1c5",
  "Timestamp" : 1.7479249130545738E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18508]:
{
  "BatchId" : "8f0bd673-84cc-4e86-a8b5-7a5dc5ec8d82",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "d6e9cce8-4362-4b11-8d3d-e06723b4f6d6",
  "Timestamp" : 1.7479250724391136E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18509]:
{
  "BatchId" : "d0764e08-67f4-497e-b8fa-0ce1959c7f3e",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "5e198436-94ec-40eb-a5b3-b912a1612a88",
  "Timestamp" : 1.747925368994319E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18510]:
{
  "BatchId" : "881d4d06-bf89-4f81-adeb-11f9ee8890c4",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "a4c4db3c-9b48-49d9-9ca6-fb3eb9d45503",
  "Timestamp" : 1.7479261567096276E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18511]:
{
  "BatchId" : "afcc1c8b-7372-424e-afe9-859a20a51919",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "0eb8249e-1412-4419-a2bd-aa7c6627283a",
  "Timestamp" : 1.7479262332148435E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18512]:
{
  "BatchId" : "2682e131-d492-4040-b72c-727598173422",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "cdccc4c6-cfed-4d28-b8c8-10c9cd23a996",
  "Timestamp" : 1.74792636490034E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18513]:
{
  "BatchId" : "47c83158-5cb1-4dbe-93c8-123750e2a3f9",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "ced1825a-999c-4bea-bbad-8bacf946b70b",
  "Timestamp" : 1.747926471170222E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18514]:
{
  "BatchId" : "65335e59-02b8-4e89-a6c0-2d7b113739c5",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "07824084-2304-4b86-b7cd-34f2d0c09de5",
  "Timestamp" : 1.7479264781200676E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18515]:
{
  "BatchId" : "3b9d3738-d3a6-4608-8ea7-94d29433553c",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "58542630-fbd7-472b-b73d-1cc987039e3c",
  "Timestamp" : 1.7479264840631814E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
New message [Key=null, Offset=18516]:
{
  "BatchId" : "137b68b2-f9c6-4053-b54f-7672d4c9f2f0",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "28c4e00d-3f93-4e36-8930-526a2f49d100",
  "Timestamp" : 1.7479264912132933E9,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
