package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import com.nedbank.kafka.filemanage.model.MetaDataInfo;
import com.nedbank.kafka.filemanage.model.PayloadInfo;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    private final BlobStorageService blobStorageService;

    public KafkaListenerService(BlobStorageService blobStorageService) {
        this.blobStorageService = blobStorageService;
    }

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.group.id}")
    public void listen(List<String> messages) {
        try {
            SummaryPayload payload = processMessages(messages);
            // Do something with payload, e.g., send to another topic or service
            logger.info("Processed batch with BatchId: {}", payload.getHeader().getBatchId());
        } catch (Exception e) {
            logger.error("Error processing Kafka messages", e);
        }
    }

    private SummaryPayload processMessages(List<String> allMessages) throws IOException {
        List<CustomerSummary> customerSummaries = new ArrayList<>();
        String batchId = null;
        String jobName = null;

        for (String message : allMessages) {
            JsonNode root = objectMapper.readTree(message);

            if (batchId == null) {
                batchId = safeGetText(root, "BatchId", false);
            }
            if (jobName == null) {
                jobName = safeGetText(root, "JobName", false);
            }

            JsonNode batchFilesNode = root.get("BatchFiles");
            if (batchFilesNode == null || !batchFilesNode.isArray()) {
                logger.warn("No BatchFiles array found in message.");
                continue;
            }

            for (JsonNode fileNode : batchFilesNode) {
                String objectId = safeGetText(fileNode, "ObjectId", false);
                String blobUrl = safeGetText(fileNode, "BlobUrl", false);
                String filename = safeGetText(fileNode, "Filename", false);
                String validationStatus = safeGetText(fileNode, "ValidationStatus", false);
                String repositoryId = safeGetText(fileNode, "RepositoryId", false);

                if (objectId == null || blobUrl == null) {
                    logger.warn("Skipping file entry due to missing ObjectId or BlobUrl.");
                    continue;
                }

                try {
                    blobStorageService.uploadFileAndGenerateSasUrl(blobUrl, batchId, objectId);
                } catch (Exception e) {
                    logger.warn("Blob upload failed for {}: {}", blobUrl, e.getMessage());
                }

                String extension = getFileExtension(blobUrl);

                CustomerSummary.FileDetail fileDetail = new CustomerSummary.FileDetail();
                fileDetail.setObjectId(objectId);
                fileDetail.setFileLocation(blobUrl);
                fileDetail.setFileUrl("https://" + azureBlobStorageAccount + "/" + blobUrl);
                fileDetail.setEncrypted(isEncrypted(blobUrl, extension));
                fileDetail.setStatus(validationStatus != null ? validationStatus : "OK");
                fileDetail.setType(determineType(blobUrl, extension));
                fileDetail.setRepositoryId(repositoryId);

                // Use objectId as customerId for grouping
                String customerId = objectId;
                CustomerSummary customer = customerSummaries.stream()
                        .filter(c -> c.getCustomerId().equals(customerId))
                        .findFirst()
                        .orElseGet(() -> {
                            CustomerSummary newCustomer = new CustomerSummary();
                            newCustomer.setCustomerId(customerId);
                            newCustomer.setAccountNumber(""); // If applicable
                            newCustomer.setFiles(new ArrayList<>());
                            customerSummaries.add(newCustomer);
                            return newCustomer;
                        });

                customer.getFiles().add(fileDetail);
            }
        }

        // Construct header info from first message
        JsonNode firstMessage = objectMapper.readTree(allMessages.get(0));
        HeaderInfo headerInfo = new HeaderInfo();
        headerInfo.setBatchId(safeGetText(firstMessage, "BatchId", false));
        headerInfo.setTenantCode(safeGetText(firstMessage, "TenantCode", false));
        headerInfo.setChannelID(safeGetText(firstMessage, "ChannelID", false));
        headerInfo.setAudienceID(safeGetText(firstMessage, "AudienceID", false));
        headerInfo.setSourceSystem(safeGetText(firstMessage, "SourceSystem", false));
        headerInfo.setProduct(safeGetText(firstMessage, "Product", false));
        headerInfo.setJobName(jobName != null ? jobName : "");
        headerInfo.setTimestamp(new Date().toString());

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);

        PayloadInfo payloadInfo = new PayloadInfo();
        payloadInfo.setPrintFiles(Collections.emptyList()); // Adjust if needed
        summaryPayload.setPayload(payloadInfo);

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);
        summaryPayload.setMetadata(metaDataInfo);

        return summaryPayload;
    }

    // Helper to safely get text fields with case-sensitive key
    private String safeGetText(JsonNode node, String fieldName, boolean required) {
        if (node != null && node.has(fieldName) && !node.get(fieldName).isNull()) {
            String value = node.get(fieldName).asText();
            return value.equalsIgnoreCase("null") ? null : value;
        }
        return required ? "" : null;
    }

    private String getFileExtension(String fileName) {
        if (fileName == null) return "";
        int lastDot = fileName.lastIndexOf('.');
        return lastDot == -1 ? "" : fileName.substring(lastDot + 1);
    }

    private boolean isEncrypted(String blobUrl, String extension) {
        // Add your encryption detection logic here, dummy example:
        return extension.equalsIgnoreCase("enc");
    }

    private String determineType(String blobUrl, String extension) {
        // Determine file type logic, dummy example:
        if ("csv".equalsIgnoreCase(extension)) {
            return "CSV";
        } else if ("json".equalsIgnoreCase(extension)) {
            return "JSON";
        }
        return "UNKNOWN";
    }
}
