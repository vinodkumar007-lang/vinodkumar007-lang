package com.example.demo.listener;

import com.example.demo.service.BlobStorageService;
import com.example.demo.service.SummaryJsonWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;
    private final BlobStorageService blobStorageService;
    private final SummaryJsonWriter summaryJsonWriter;

    private final Set<String> processedMessageIds = new HashSet<>();

    public KafkaListenerService(KafkaConsumer<String, String> consumer,
                                ObjectMapper objectMapper,
                                BlobStorageService blobStorageService,
                                SummaryJsonWriter summaryJsonWriter) {
        this.consumer = consumer;
        this.objectMapper = objectMapper;
        this.blobStorageService = blobStorageService;
        this.summaryJsonWriter = summaryJsonWriter;
    }

    public SummaryResponse listen() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        List<ProcessedFile> processedFiles = new ArrayList<>();
        List<PrintFile> printFiles = new ArrayList<>();

        String batchId = UUID.randomUUID().toString();
        String fileName = "summary.json";

        Header header = null;
        Metadata metadata = null;

        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();

            try {
                JsonNode rootNode = objectMapper.readTree(message);
                String messageId = rootNode.path("id").asText();

                if (processedMessageIds.contains(messageId)) {
                    continue;
                }

                processedMessageIds.add(messageId);

                JsonNode dataNode = rootNode.path("data");

                String blobUrl = dataNode.path("blobUrl").asText();
                String sourceSystem = dataNode.path("sourceSystem").asText();
                String objectId = dataNode.path("objectId").asText();
                String consumerReference = dataNode.path("consumerReference").asText();
                String processReference = dataNode.path("processReference").asText();
                String timestamp = dataNode.path("timestamp").asText();
                String eventOutcomeCode = dataNode.path("eventOutcomeCode").asText();
                String eventOutcomeDescription = dataNode.path("eventOutcomeDescription").asText();

                // Copy file to Azure Blob Storage
                String destPath = batchId + "/" + objectId;

                Map<String, String> metaMap = new HashMap<>();
                metaMap.put("sourceSystem", sourceSystem);
                metaMap.put("consumerReference", consumerReference);
                metaMap.put("processReference", processReference);
                metaMap.put("timestamp", timestamp);

                try {
                    blobStorageService.copyFileFromUrlToBlob(blobUrl, destPath, metaMap);
                } catch (Exception e) {
                    logger.warn("Failed to copy blob from URL {}: {}", blobUrl, e.getMessage());
                }

                // Build processed file POJO
                ProcessedFile processedFile = new ProcessedFile(blobUrl, sourceSystem, objectId,
                        consumerReference, processReference, timestamp,
                        eventOutcomeCode, eventOutcomeDescription);
                processedFiles.add(processedFile);

                // Build print file POJO if available
                if (dataNode.has("printFileURL")) {
                    printFiles.add(new PrintFile(dataNode.path("printFileURL").asText()));
                }

                // Build header once
                if (header == null) {
                    header = new Header(batchId, fileName, Instant.now().toString());
                }

                // Build metadata once from first processed file
                if (metadata == null) {
                    metadata = new Metadata(sourceSystem, consumerReference, processReference, timestamp);
                }

            } catch (Exception e) {
                logger.error("Failed to process Kafka message: {}", e.getMessage(), e);
            }
        }

        if (processedFiles.isEmpty()) {
            return new SummaryResponse("No new Kafka messages to process", "success", null);
        }

        // Create summary.json file with the SummaryJsonWriter utility
        File summaryFile = summaryJsonWriter.appendToSummaryJson(batchId, header, processedFiles, printFiles);

        String summaryBlobUrl = null;
        try {
            summaryBlobUrl = blobStorageService.uploadSummaryJson(summaryFile,
                    "summaries/" + UUID.randomUUID() + "-summary.json");
        } catch (Exception e) {
            logger.error("Failed to upload summary.json: {}", e.getMessage(), e);
        }

        SummaryPayload summaryPayload = new SummaryPayload(header, metadata, processedFiles, printFiles, summaryBlobUrl);
        return new SummaryResponse("Kafka messages processed successfully", "success", summaryPayload);
    }


    // ---- POJO Classes ----

    public static class SummaryResponse {
        private String message;
        private String status;
        private SummaryPayload summaryPayload;

        public SummaryResponse() {}

        public SummaryResponse(String message, String status, SummaryPayload summaryPayload) {
            this.message = message;
            this.status = status;
            this.summaryPayload = summaryPayload;
        }

        // Getters and setters
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public SummaryPayload getSummaryPayload() { return summaryPayload; }
        public void setSummaryPayload(SummaryPayload summaryPayload) { this.summaryPayload = summaryPayload; }
    }

    public static class SummaryPayload {
        private Header header;
        private Metadata metadata;
        private List<ProcessedFile> processedFiles;
        private List<PrintFile> printFiles;
        private String summaryFileURL;

        public SummaryPayload() {}

        public SummaryPayload(Header header, Metadata metadata, List<ProcessedFile> processedFiles,
                              List<PrintFile> printFiles, String summaryFileURL) {
            this.header = header;
            this.metadata = metadata;
            this.processedFiles = processedFiles;
            this.printFiles = printFiles;
            this.summaryFileURL = summaryFileURL;
        }

        public Header getHeader() { return header; }
        public void setHeader(Header header) { this.header = header; }

        public Metadata getMetadata() { return metadata; }
        public void setMetadata(Metadata metadata) { this.metadata = metadata; }

        public List<ProcessedFile> getProcessedFiles() { return processedFiles; }
        public void setProcessedFiles(List<ProcessedFile> processedFiles) { this.processedFiles = processedFiles; }

        public List<PrintFile> getPrintFiles() { return printFiles; }
        public void setPrintFiles(List<PrintFile> printFiles) { this.printFiles = printFiles; }

        public String getSummaryFileURL() { return summaryFileURL; }
        public void setSummaryFileURL(String summaryFileURL) { this.summaryFileURL = summaryFileURL; }
    }

    public static class Header {
        private String batchID;
        private String fileName;
        private String timestamp;

        public Header() {}

        public Header(String batchID, String fileName, String timestamp) {
            this.batchID = batchID;
            this.fileName = fileName;
            this.timestamp = timestamp;
        }

        public String getBatchID() { return batchID; }
        public void setBatchID(String batchID) { this.batchID = batchID; }

        public String getFileName() { return fileName; }
        public void setFileName(String fileName) { this.fileName = fileName; }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    }

    public static class Metadata {
        private String sourceSystem;
        private String consumerReference;
        private String processReference;
        private String timestamp;

        public Metadata() {}

        public Metadata(String sourceSystem, String consumerReference,
                        String processReference, String timestamp) {
            this.sourceSystem = sourceSystem;
            this.consumerReference = consumerReference;
            this.processReference = processReference;
            this.timestamp = timestamp;
        }

        public String getSourceSystem() { return sourceSystem; }
        public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }

        public String getConsumerReference() { return consumerReference; }
        public void setConsumerReference(String consumerReference) { this.consumerReference = consumerReference; }

        public String getProcessReference() { return processReference; }
        public void setProcessReference(String processReference) { this.processReference = processReference; }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    }

    public static class ProcessedFile {
        private String blobUrl;
        private String sourceSystem;
        private String objectId;
        private String consumerReference;
        private String processReference;
        private String timestamp;
        private String eventOutcomeCode;
        private String eventOutcomeDescription;

        public ProcessedFile() {}

        public ProcessedFile(String blobUrl, String sourceSystem, String objectId,
                             String consumerReference, String processReference, String timestamp,
                             String eventOutcomeCode, String eventOutcomeDescription) {
            this.blobUrl = blobUrl;
            this.sourceSystem = sourceSystem;
            this.objectId = objectId;
            this.consumerReference = consumerReference;
            this.processReference = processReference;
            this.timestamp = timestamp;
            this.eventOutcomeCode = eventOutcomeCode;
            this.eventOutcomeDescription = eventOutcomeDescription;
        }

        public String getBlobUrl() { return blobUrl; }
        public void setBlobUrl(String blobUrl) { this.blobUrl = blobUrl; }

        public String getSourceSystem() { return sourceSystem; }
        public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }

        public String getObjectId() { return objectId; }
        public void setObjectId(String objectId) { this.objectId = objectId; }

        public String getConsumerReference() { return consumerReference; }
        public void setConsumerReference(String consumerReference) { this.consumerReference = consumerReference; }

        public String getProcessReference() { return processReference; }
        public void setProcessReference(String processReference) { this.processReference = processReference; }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

        public String getEventOutcomeCode() { return eventOutcomeCode; }
        public void setEventOutcomeCode(String eventOutcomeCode) { this.eventOutcomeCode = eventOutcomeCode; }

        public String getEventOutcomeDescription() { return eventOutcomeDescription; }
        public void setEventOutcomeDescription(String eventOutcomeDescription) { this.eventOutcomeDescription = eventOutcomeDescription; }
    }

    public static class PrintFile {
        private String printFileURL;

        public PrintFile() {}

        public PrintFile(String printFileURL) {
            this.printFileURL = printFileURL;
        }

        public String getPrintFileURL() { return printFileURL; }
        public void setPrintFileURL(String printFileURL) { this.printFileURL = printFileURL; }
    }

}
