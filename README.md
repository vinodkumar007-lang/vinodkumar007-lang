package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
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
        Map<String, Object> summaryResponse = new HashMap<>();
        List<Map<String, Object>> processedBatchSummaries = new ArrayList<>();

        try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
            consumer.assign(Collections.singletonList(new TopicPartition(inputTopic, 0)));
            consumer.seekToBeginning(Collections.singletonList(new TopicPartition(inputTopic, 0)));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing record with key: {}, value: {}", record.key(), record.value());
                Map<String, Object> recordSummary = handleMessage(record.value());
                if (recordSummary != null) {
                    processedBatchSummaries.add(recordSummary);
                }
            }

            if (!processedBatchSummaries.isEmpty()) {
                summaryResponse.put("processedBatchSummaries", processedBatchSummaries);
                logger.info("Processed {} messages from Kafka topic: {}", processedBatchSummaries.size(), inputTopic);
            }

            return summaryResponse;

        } catch (org.apache.kafka.common.errors.RecordTooLargeException e) {
            throw new ResponseStatusException(HttpStatus.valueOf(701), "Message too large for Kafka topic.", e);
        } catch (org.apache.kafka.common.errors.SslAuthenticationException e) {
            throw new ResponseStatusException(HttpStatus.valueOf(530), "SSL authentication failure", e);
        } catch (org.apache.kafka.common.errors.AuthorizationException e) {
            throw new ResponseStatusException(HttpStatus.valueOf(421), "Access denied", e);
        } catch (Exception e) {
            logger.error("Runtime exception in Kafka listener", e);
            throw new ResponseStatusException(HttpStatus.valueOf(451), "Runtime error from Kafka or Camel framework", e);
        }
    }

    private Map<String, Object> handleMessage(String message) throws Exception {
        JsonNode root;
        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            logger.warn("Initial parse failed, attempting fallback: {}", message);
            message = convertPojoToJson(message);
            root = objectMapper.readTree(message);
        }

        String batchId = extractField(root, "consumerReference");
        if (batchId == null) throw new ResponseStatusException(HttpStatus.valueOf(601), "Missing batchId");

        JsonNode batchFilesNode = root.get("batchFiles");
        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.valueOf(601), "No batch files found.");
        }

        String blobUrl = "file://./output";

        Map<String, Object> summaryResponse = buildSummaryPayload(root, batchId, blobUrl, batchFilesNode);
        String summaryMessage = objectMapper.writeValueAsString(summaryResponse);

        kafkaTemplate.send(outputTopic, batchId, summaryMessage);
        logger.info("Summary published to Kafka topic: {} with message: {}", outputTopic, summaryMessage);

        return summaryResponse;
    }

    private Map<String, Object> buildSummaryPayload(JsonNode root, String batchId, String blobUrl, JsonNode batchFilesNode) {
        List<ProcessedFileInfo> processedFiles = new ArrayList<>();
        List<CustomerSummary> customerSummaries = new ArrayList<>();

        Set<String> archived = new HashSet<>();
        Set<String> emailed = new HashSet<>();
        Set<String> mobstat = new HashSet<>();
        Set<String> print = new HashSet<>();

        String fileName = extractField(batchFilesNode.get(0), "fileName");
        String jobName = extractField(batchFilesNode.get(0), "jobName");

        for (JsonNode fileNode : batchFilesNode) {
            String objectId = extractField(fileNode, "ObjectId");
            String fileLocation = extractField(fileNode, "fileLocation");
            String extension = getFileExtension(fileLocation).toLowerCase();
            String customerId = objectId.split("_")[0];

            String dynamicFileUrl = "file://" + fileLocation;

            CustomerSummary.FileDetail fileDetail = new CustomerSummary.FileDetail();
            fileDetail.setObjectId(objectId);
            fileDetail.setFileUrl(dynamicFileUrl);
            fileDetail.setFileLocation(fileLocation);
            fileDetail.setStatus(extension.equals(".ps") ? "failed" : "OK");
            fileDetail.setEncrypted(isEncrypted(fileLocation, extension));
            fileDetail.setType(determineType(fileLocation, extension));

            if (fileLocation.contains("archive")) archived.add(customerId);
            if (fileLocation.contains("mobstat")) mobstat.add(customerId);
            if (fileLocation.contains("email")) emailed.add(customerId);
            if (extension.equals(".ps")) print.add(customerId);

            processedFiles.add(new ProcessedFileInfo(objectId, dynamicFileUrl));

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

            customer.getFiles().add(fileDetail);
        }

        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("totalCustomersProcessed", customerSummaries.size());
        totals.put("totalArchived", archived.size());
        totals.put("totalEmailed", emailed.size());
        totals.put("totalMobstat", mobstat.size());
        totals.put("totalPrint", print.size());

        // Golden thread data
        HeaderInfo header = new HeaderInfo();
        header.setTenantCode(extractField(root, "tenantCode"));
        header.setChannelID(extractField(root, "channelID"));
        header.setAudienceID(extractField(root, "audienceID"));
        header.setTimestamp(ZonedDateTime.now().toString());
        header.setSourceSystem(extractField(root, "sourceSystem"));
        header.setProduct(extractField(root, "product"));
        header.setJobName(jobName);

        MetadataInfo metadata = new MetadataInfo();
        metadata.setTotalFilesProcessed(batchFilesNode.size());
        metadata.setProcessingStatus("Success");
        metadata.setEventOutcomeCode("Success");
        metadata.setEventOutcomeDescription("All customer PDFs processed successfully");

        PayloadInfo payload = new PayloadInfo();
        payload.setUniqueConsumerRef(extractField(root, "consumerReference"));
        payload.setUniqueECPBatchRef(extractField(root, "ecpBatchId"));
        payload.setFilenetObjectID(Arrays.asList(UUID.randomUUID().toString()));
        payload.setRepositoryID("Legacy");
        payload.setRunPriority("High");
        payload.setEventID("E12345");
        payload.setEventType("Completion");
        payload.setRestartKey("Key123");

        String summaryFilePath = writeSummaryToFile(batchId, fileName, jobName, customerSummaries, totals);

        SummaryPayload summary = new SummaryPayload();
        summary.setBatchID(batchId);
        summary.setHeader(header);
        summary.setMetadata(metadata);
        summary.setPayload(payload);
        summary.setProcessedFiles(processedFiles);
        summary.setSummaryFileURL(summaryFilePath);
        summary.setTimestamp(ZonedDateTime.now().toString());

        return objectMapper.convertValue(summary, Map.class);
    }

    private String writeSummaryToFile(String batchId, String fileName, String jobName,
                                      List<CustomerSummary> customers, Map<String, Object> totals) {
        try {
            String userHome = System.getProperty("user.home");
            File dir = new File(userHome + File.separator + "summary_outputs");
            if (!dir.exists()) dir.mkdirs();

            String path = dir + File.separator + "summary_" + batchId + ".json";
            Map<String, Object> summaryJson = new LinkedHashMap<>();
            summaryJson.put("fileName", fileName);
            summaryJson.put("jobName", jobName);
            summaryJson.put("batchId", batchId);
            summaryJson.put("timestamp", new Date().toString());
            summaryJson.put("customers", customers);
            summaryJson.put("totals", totals);

            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(path), summaryJson);
            logger.info("Summary JSON written to: {}", path);

            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                Runtime.getRuntime().exec(new String[]{"cmd", "/c", "start", "", path});
            }

            return "file://" + path.replace("\\", "/");
        } catch (IOException e) {
            logger.error("Failed to write summary JSON", e);
            return null;
        }
    }

    private boolean isEncrypted(String location, String ext) {
        return (ext.equals(".pdf") || ext.equals(".html") || ext.equals(".txt"))
                && (location.toLowerCase().contains("mobstat") || location.toLowerCase().contains("email"));
    }

    private String determineType(String location, String ext) {
        if (location.contains("mobstat")) return "pdf_mobstat";
        if (location.contains("archive")) return "pdf_archive";
        if (location.contains("email")) {
            if (ext.equals(".html")) return "html_email";
            if (ext.equals(".txt")) return "txt_email";
            return "pdf_email";
        }
        if (ext.equals(".ps")) return "ps_print";
        return "unknown";
    }

    private String getFileExtension(String file) {
        int lastDot = file.lastIndexOf('.');
        return lastDot > 0 ? file.substring(lastDot) : "";
    }

    private String extractField(JsonNode node, String field) {
        return (node.has(field) && !node.get(field).isNull()) ? node.get(field).asText() : null;
    }

    private String convertPojoToJson(String raw) {
        raw = raw.trim();
        if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
            raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
        }
        raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
        raw = raw.replaceAll(":([a-zA-Z0-9_]+)", ":\"$1\"");
        raw = raw.replaceAll("objectId=\\{([^}]+)}", "\"objectId\": \"$1\"");
        raw = raw.replace("],", "], ");
        return "{" + raw + "}";
    }
}
