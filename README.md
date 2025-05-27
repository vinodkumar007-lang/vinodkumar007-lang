package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.config.ProxySetup;
import com.nedbank.kafka.filemanage.exception.CustomAppException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    private final RestTemplate restTemplate;
    private final ProxySetup proxySetup;

    @Value("${vault.hashicorp.url}")
    private String VAULT_URL;

    @Value("${vault.hashicorp.namespace}")
    private String VAULT_NAMESPACE;

    @Value("${vault.hashicorp.passwordDev}")
    private String passwordDev;

    @Value("${vault.hashicorp.passwordNbhDev}")
    private String passwordNbhDev;

    @Value("${use.proxy:false}")
    private boolean useProxy;

    private String accountKey;
    private String accountName;
    private String containerName;

    public BlobStorageService(RestTemplate restTemplate, ProxySetup proxySetup) {
        this.restTemplate = restTemplate;
        this.proxySetup = proxySetup;
    }

    private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) {
            return; // already initialized
        }
        String token = getVaultToken();
        accountKey = getSecretFromVault("account_key", token);
        accountName = getSecretFromVault("account_name", token);
        containerName = getSecretFromVault("container_name", token);
        logger.info("Vault secrets initialized for Blob Storage");
    }

    public String copyFileFromUrlToBlob(
            String sourceUrl,
            String sourceSystem,
            String batchId,
            String consumerReference,
            String processReference,
            String timestamp,
            String fileName) {

        try {
            if (sourceUrl == null || sourceSystem == null || batchId == null
                    || consumerReference == null || processReference == null
                    || timestamp == null || fileName == null) {
                throw new CustomAppException("Required parameters missing", 400, HttpStatus.BAD_REQUEST);
            }

            initSecrets();

            String blobName = String.format("%s/input/%s/%s/%s_%s/%s",
                    sourceSystem,
                    timestamp,
                    batchId,
                    consumerReference.replaceAll("[{}]", ""),
                    processReference.replaceAll("[{}]", ""),
                    fileName);

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

            try (InputStream inputStream = restTemplate.getForObject(sourceUrl, InputStream.class)) {
                if (inputStream == null) {
                    throw new CustomAppException("Unable to read source file from URL: " + sourceUrl, 404, HttpStatus.NOT_FOUND);
                }

                byte[] data = inputStream.readAllBytes();

                targetBlobClient.upload(new java.io.ByteArrayInputStream(data), data.length, true);

                logger.info("✅ Copied '{}' to '{}'", sourceUrl, targetBlobClient.getBlobUrl());
            }

            return targetBlobClient.getBlobUrl();

        } catch (CustomAppException cae) {
            throw cae;
        } catch (Exception e) {
            logger.error("❌ Error copying file from URL: {}", e.getMessage(), e);
            throw new CustomAppException("Error copying file from URL", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String uploadSummaryJson(
            String sourceSystem,
            String batchId,
            String timestamp,
            String summaryJsonContent) {

        try {
            if (summaryJsonContent == null || sourceSystem == null || batchId == null || timestamp == null) {
                throw new CustomAppException("Required parameters missing for summary upload", 400, HttpStatus.BAD_REQUEST);
            }

            initSecrets();

            String blobName = String.format("%s/summary/%s/%s/summary.json",
                    sourceSystem,
                    timestamp,
                    batchId);

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient summaryBlobClient = containerClient.getBlobClient(blobName);

            byte[] jsonBytes = summaryJsonContent.getBytes(StandardCharsets.UTF_8);

            summaryBlobClient.upload(new java.io.ByteArrayInputStream(jsonBytes), jsonBytes.length, true);

            logger.info("✅ Uploaded summary.json to '{}'", summaryBlobClient.getBlobUrl());

            return summaryBlobClient.getBlobUrl();

        } catch (CustomAppException cae) {
            throw cae;
        } catch (Exception e) {
            logger.error("❌ Error uploading summary.json: {}", e.getMessage(), e);
            throw new CustomAppException("Error uploading summary.json", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String getVaultToken() {
        try {
            String url = VAULT_URL + "/v1/auth/userpass/login/espire_dev";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-vault-namespace", VAULT_NAMESPACE);

            Map<String, String> body = new HashMap<>();
            body.put("password", passwordDev);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

            JSONObject json = new JSONObject(Objects.requireNonNull(response.getBody()));
            return json.getJSONObject("auth").getString("client_token");
        } catch (Exception e) {
            logger.error("❌ Vault token fetch failed: {}", e.getMessage());
            throw new CustomAppException("Vault authentication error", 401, HttpStatus.UNAUTHORIZED, e);
        }
    }

    private String getSecretFromVault(String key, String token) {
        try {
            String url = VAULT_URL + "/v1/Store_Dev/10099";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-vault-namespace", VAULT_NAMESPACE);
            headers.set("x-vault-token", token);

            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(headers), String.class);

            JSONObject json = new JSONObject(Objects.requireNonNull(response.getBody()));
            return json.getJSONObject("data").getString(key);
        } catch (Exception e) {
            logger.error("❌ Failed to retrieve secret from Vault: {}", e.getMessage());
            throw new CustomAppException("Vault secret fetch error", 403, HttpStatus.FORBIDDEN, e);
        }
    }

    public String getFileNameFromUrl(String url) {
        if (url == null || url.isEmpty()) return "unknownFile";
        int lastSlashIndex = url.lastIndexOf('/');
        if (lastSlashIndex < 0) return url;
        return url.substring(lastSlashIndex + 1);
    }
}

package com.nedbank.kafka.filemanage.service;

import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class SummaryJsonWriter {

    // Using synchronized sets for thread safety
    private static final Set<String> processedFileKeys = new HashSet<>();
    private static final Set<String> printFileUrls = new HashSet<>();

    /**
     * Append a SummaryPayload entry into the summary.json file, avoiding duplicates.
     * Thread-safe for multiple appends.
     */
    public synchronized static void appendToSummaryJson(File summaryFile, SummaryPayload summaryPayload) throws IOException {
        JSONObject rootJson;
        if (summaryFile.exists()) {
            String existingContent = readFileContent(summaryFile);
            if (existingContent.isEmpty()) {
                rootJson = new JSONObject();
            } else {
                rootJson = new JSONObject(existingContent);
            }
        } else {
            rootJson = new JSONObject();
        }

        // header & metadata
        JSONObject headerJson = new JSONObject(summaryPayload.getHeader());
        JSONObject metadataJson = new JSONObject(summaryPayload.getMetadata());
        JSONObject payloadJson = new JSONObject(summaryPayload.getPayload());

        rootJson.put("header", headerJson);
        rootJson.put("metadata", metadataJson);
        rootJson.put("payload", payloadJson);

        // processedFiles array
        JSONArray processedFilesArray = rootJson.optJSONArray("processedFiles");
        if (processedFilesArray == null) processedFilesArray = new JSONArray();

        // Append unique processedFiles
        for (var pf : summaryPayload.getProcessedFiles()) {
            String uniqueKey = pf.getSourceURL() + pf.getFileName();
            if (!processedFileKeys.contains(uniqueKey)) {
                processedFileKeys.add(uniqueKey);
                JSONObject pfJson = new JSONObject();
                pfJson.put("sourceURL", pf.getSourceURL());
                pfJson.put("fileName", pf.getFileName());
                pfJson.put("status", pf.getStatus());
                pfJson.put("blobURL", pf.getBlobURL());
                pfJson.put("batchId", pf.getBatchId());
                processedFilesArray.put(pfJson);
            }
        }
        rootJson.put("processedFiles", processedFilesArray);

        // printFiles array
        JSONArray printFilesArray = rootJson.optJSONArray("printFiles");
        if (printFilesArray == null) printFilesArray = new JSONArray();

        for (var pf : summaryPayload.getPrintFiles()) {
            if (!printFileUrls.contains(pf.getPrintFileURL())) {
                printFileUrls.add(pf.getPrintFileURL());
                JSONObject pfJson = new JSONObject();
                pfJson.put("printFileURL", pf.getPrintFileURL());
                printFilesArray.put(pfJson);
            }
        }
        rootJson.put("printFiles", printFilesArray);

        // Save back to file
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(summaryFile), StandardCharsets.UTF_8))) {
            writer.write(rootJson.toString(4));
        }
    }

    private static String readFileContent(File file) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(file), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while((line = br.readLine()) != null) sb.append(line).append("\n");
            return sb.toString().trim();
        }
    }

    /**
     * Reset all internal sets to allow fresh processing
     */
    public synchronized static void reset() {
        processedFileKeys.clear();
        printFileUrls.clear();
    }
}

package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.kafka.clients.consumer.*;
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
import java.time.Instant;
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

    private final File summaryFile = new File(System.getProperty("user.home"), "summary.json");
    private final Map<TopicPartition, Long> lastProcessedOffsets = new HashMap<>();

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    /**
     * Reads new Kafka messages from input topic, processes them into SummaryPayload,
     * copies files in Blob Storage, updates summary.json, uploads summary file,
     * sends processed payloads to output topic, and returns a batch response.
     */
    public Map<String, Object> listen() {
        logger.info("Starting to listen for Kafka messages...");
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        List<SummaryPayload> processedPayloads = new ArrayList<>();
        Map<TopicPartition, Long> newOffsets = new HashMap<>();

        try {
            // Assign partitions and seek to correct offsets to avoid reprocessing
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(inputTopic).forEach(p -> partitions.add(new TopicPartition(p.topic(), p.partition())));
            consumer.assign(partitions);
            logger.info("Assigned to partitions: {}", partitions);

            for (TopicPartition partition : partitions) {
                if (lastProcessedOffsets.containsKey(partition)) {
                    long nextOffset = lastProcessedOffsets.get(partition) + 1;
                    consumer.seek(partition, nextOffset);
                    logger.info("Seeking partition {} to offset {}", partition.partition(), nextOffset);
                } else {
                    consumer.seekToBeginning(Collections.singletonList(partition));
                    logger.info("Seeking partition {} to beginning", partition.partition());
                }
            }

            // Poll messages (timeout 5 seconds)
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            logger.info("Polled {} record(s)", records.count());

            // Process each record if not already processed
            for (ConsumerRecord<String, String> record : records) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());

                if (lastProcessedOffsets.containsKey(tp) && record.offset() <= lastProcessedOffsets.get(tp)) {
                    logger.debug("Skipping already processed record at offset {}", record.offset());
                    continue;
                }

                logger.info("Processing message at offset {} from partition {}", record.offset(), record.partition());
                SummaryPayload payload = processSingleMessage(record.value());

                if (payload.getBatchId() == null || payload.getBatchId().isBlank()) {
                    logger.warn("Skipping message with missing batch ID at offset {}", record.offset());
                    continue;
                }

                processedPayloads.add(payload);
                newOffsets.put(tp, record.offset());
            }

            if (processedPayloads.isEmpty()) {
                logger.warn("No new valid messages to process.");
                return generateErrorResponse("204", "No new valid messages to process.");
            }

            // Append to summary.json on disk
            logger.info("Appending payloads to summary.json file...");
            SummaryJsonWriter.appendToSummaryJson(summaryFile, processedPayloads, azureBlobStorageAccount);

            // Upload summary.json to blob storage and get URL
            logger.info("Uploading summary.json to blob storage...");
            String summaryFileUrl = blobStorageService.uploadSummaryJson(summaryFile);
            logger.info("Summary file uploaded to: {}", summaryFileUrl);

            // Send each payload with summaryFileURL to Kafka output topic
            for (SummaryPayload payload : processedPayloads) {
                payload.setSummaryFileURL(summaryFileUrl);
                kafkaTemplate.send(outputTopic, payload.getBatchId(), objectMapper.writeValueAsString(payload));
                logger.info("Sent processed payload to Kafka topic: {}", outputTopic);
            }

            // Update last processed offsets
            lastProcessedOffsets.putAll(newOffsets);

            // Return consolidated batch response
            return buildBatchFinalResponse(processedPayloads);

        } catch (Exception e) {
            logger.error("Error processing Kafka messages", e);
            return generateErrorResponse("500", "Internal Server Error.");
        } finally {
            consumer.close();
            logger.info("Kafka consumer closed.");
        }
    }

    /**
     * Builds batch level response including individual processed payload responses.
     */
    private Map<String, Object> buildBatchFinalResponse(List<SummaryPayload> payloads) {
        Map<String, Object> batch = new LinkedHashMap<>();
        batch.put("messagesProcessed", payloads.size());

        List<Map<String, Object>> individual = new ArrayList<>();
        for (SummaryPayload payload : payloads) {
            individual.add(buildFinalResponse(payload));
        }
        batch.put("payloads", individual);
        return batch;
    }

    /**
     * Builds single payload processing response.
     */
    private Map<String, Object> buildFinalResponse(SummaryPayload payload) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("message", "Processed successfully");
        response.put("status", "SUCCESS");
        response.put("summaryPayload", payload);
        return response;
    }

    /**
     * Generates error response map with status and message.
     */
    private Map<String, Object> generateErrorResponse(String status, String message) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("status", status);
        error.put("message", message);
        return error;
    }

    /**
     * Processes a single Kafka message JSON string into SummaryPayload.
     * Copies referenced files from blob URLs into Azure Blob Storage with structured path.
     */
    private SummaryPayload processSingleMessage(String message) throws IOException {
        logger.debug("Parsing message JSON");
        JsonNode root = objectMapper.readTree(message);
        JsonNode payloadNode = root.get("Payload");

        // Extract key fields with fallback to Payload node if needed
        String sourceSystem = safeGetText(root, "sourceSystem", false);
        if (sourceSystem == null && payloadNode != null) {
            sourceSystem = safeGetText(payloadNode, "sourceSystem", false);
        }
        if (sourceSystem == null) sourceSystem = "DEBTMAN";

        String jobName = safeGetText(root, "JobName", false);
        String batchId = safeGetText(root, "BatchId", true);
        if (batchId == null) batchId = UUID.randomUUID().toString();
        String timestamp = Instant.now().toString();

        String consumerReference = safeGetText(root, "consumerReference", false);
        if (consumerReference == null && payloadNode != null) {
            consumerReference = safeGetText(payloadNode, "consumerReference", false);
        }
        if (consumerReference == null) consumerReference = "unknownConsumer";

        String processReference = safeGetText(root, "eventID", false);
        if (processReference == null && payloadNode != null) {
            processReference = safeGetText(payloadNode, "eventID", false);
        }
        if (processReference == null) processReference = "unknownProcess";

        // Build list of CustomerSummary objects with file details
        List<CustomerSummary> customerSummaries = new ArrayList<>();
        JsonNode batchFiles = root.get("BatchFiles");
        if (batchFiles != null && batchFiles.isArray()) {
            for (JsonNode file : batchFiles) {
                String blobUrl = safeGetText(file, "BlobUrl", true);
                String objectId = safeGetText(file, "ObjectId", true);
                String status = safeGetText(file, "ValidationStatus", false);

                if (blobUrl == null || objectId == null) continue;

                logger.info("Copying file from blob URL to storage: {}", blobUrl);
                String newBlobUrl = blobStorageService.uploadFileAndReturnLocation(
                        sourceSystem, blobUrl, batchId, objectId,
                        consumerReference, processReference, timestamp
                );
                logger.info("Copied file to: {}", newBlobUrl);

                CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
                detail.setObjectId(objectId);
                detail.setFileLocation(blobUrl);
                detail.setFileUrl(newBlobUrl);
                detail.setEncrypted(blobUrl.endsWith(".enc"));
                detail.setStatus(status != null ? status : "OK");
                detail.setType(determineType(blobUrl));

                // Find existing customer or create new
                CustomerSummary customer = customerSummaries.stream()
                        .filter(c -> c.getCustomerId().equals(objectId))
                        .findFirst()
                        .orElseGet(() -> {
                            CustomerSummary c = new CustomerSummary();
                            c.setCustomerId(objectId);
                            c.setFiles(new ArrayList<>());
                            customerSummaries.add(c);
                            return c;
                        });

                customer.getFiles().add(detail);
            }
        }

        HeaderInfo header = buildHeader(root.get("Header") != null ? root.get("Header") : root, jobName);
        if (header.getBatchId() == null) header.setBatchId(batchId);
        header.setProduct(safeGetText(root, "product", false));

        PayloadInfo payloadInfo = new PayloadInfo();
        if (payloadNode != null && !payloadNode.isNull()) {
            payloadInfo.setUniqueConsumerRef(safeGetText(payloadNode, "uniqueConsumerRef", false));
            payloadInfo.setUniqueECPBatchRef(safeGetText(payloadNode, "uniqueECPBatchRef", false));
            payloadInfo.setRunPriority(safeGetText(payloadNode, "runPriority", false));
            payloadInfo.setEventID(safeGetText(payloadNode, "eventID", false));
            payloadInfo.setEventType(safeGetText(payloadNode, "eventType", false));
            payloadInfo.setRestartKey(safeGetText(payloadNode, "restartKey", false));
            payloadInfo.setBlobURL(safeGetText(payloadNode, "blobURL", false));
            payloadInfo.setEventOutcomeCode(safeGetText(payloadNode, "eventOutcomeCode", false));
            payloadInfo.setEventOutcomeDescription(safeGetText(payloadNode, "eventOutcomeDescription", false));

            JsonNode printFiles = payloadNode.get("printFiles");
            if (printFiles != null && printFiles.isArray()) {
                List<String> pfList = new ArrayList<>();
                for (JsonNode pf : printFiles) {
                    pfList.add(pf.asText());
                }
                payloadInfo.setPrintFiles(pfList);
            }
        }

        MetaDataInfo meta = new MetaDataInfo();
        meta.setTotalCustomers(customerSummaries.size());
        meta.setTotalFiles(customerSummaries.stream().mapToInt(c -> c.getFiles().size()).sum());

        SummaryPayload payload = new SummaryPayload();
        payload.setJobName(jobName);
        payload.setBatchId(batchId);
        // You may uncomment if you want to include detailed customer summary and payload info
        // payload.setCustomerSummary(customerSummaries);
        // payload.setPayload(payloadInfo);
        payload.setHeader(header);
        payload.setMetaData(meta);

        logger.debug("Finished building SummaryPayload");
        return payload;
    }

    /**
     * Builds HeaderInfo POJO from JSON node.
     */
    private HeaderInfo buildHeader(JsonNode node, String jobName) {
        HeaderInfo header = new HeaderInfo();
        header.setBatchId(safeGetText(node, "BatchId", false));
        header.setRunPriority(safeGetText(node, "RunPriority", false));
        header.setEventID(safeGetText(node, "EventID", false));
        header.setEventType(safeGetText(node, "EventType", false));
        header.setRestartKey(safeGetText(node, "RestartKey", false));
        header.setJobName(jobName);
        return header;
    }

    /**
     * Determines file type based on file extension.
     */
    private String determineType(String path) {
        if (path.endsWith(".pdf")) return "PDF";
        if (path.endsWith(".xml")) return "XML";
        return "UNKNOWN";
    }

    /**
     * Safely extracts text field from JSON node, logs warning if mandatory and missing.
     */
    private String safeGetText(JsonNode node, String field, boolean mandatory) {
        if (node != null && node.has(field)) {
            return node.get(field).asText();
        }
        if (mandatory) {
            logger.warn("Missing mandatory field: {}", field);
        }
        return null;
    }
}
