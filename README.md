package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
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

            // Process all messages and generate the summary payload
            SummaryPayload summaryPayload = processMessages(allMessages);

            // Write the summary to a JSON file
            File jsonFile = writeSummaryToFile(summaryPayload);

            // Send the final response to Kafka
            sendFinalResponseToKafka(summaryPayload, jsonFile);

            // Return the summary payload in the response
            return Map.of("status", "success", "summaryPayload", summaryPayload);

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

        Set<String> archived = new HashSet<>();
        Set<String> emailed = new HashSet<>();
        Set<String> mobstat = new HashSet<>();
        Set<String> printed = new HashSet<>();

        for (String message : allMessages) {
            JsonNode root = objectMapper.readTree(message);
            String batchId = extractField(root, "consumerReference");
            JsonNode batchFilesNode = root.get("batchFiles");

            if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
                throw new IOException("No batch files found");
            }

            JsonNode firstFile = batchFilesNode.get(0);
            String filePath = firstFile.get("fileLocation").asText();
            String objectId = firstFile.get("ObjectId").asText();

            String sasUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);

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
                detail.setFileUrl("https://" + azureBlobStorageAccount + "/" + location);
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
