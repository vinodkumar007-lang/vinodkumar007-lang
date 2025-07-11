package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @Value("${mount.path}")
    private String mountPath;

    @Value("${kafka.topic.output}")
    private String kafkaOutputTopic;

    @Value("${rpt.max.wait.seconds}")
    private int rptMaxWaitSeconds;

    @Value("${rpt.poll.interval.millis}")
    private int rptPollIntervalMillis;

    @Value("${ot.orchestration.api.url}")
    private String otOrchestrationApiUrl;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    public KafkaListenerService(BlobStorageService blobStorageService,
                                KafkaTemplate<String, String> kafkaTemplate) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeKafkaMessage(String message) {
        try {
            logger.info("Received Kafka message.");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
            ApiResponse response = processSingleMessage(kafkaMessage);
            logger.info("Final Summary JSON: \n{}", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response.getSummaryPayload()));
            logger.info("Final API Response: \n{}", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response));
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(response));
            logger.info("Sent processed response to Kafka output topic.");
        } catch (Exception ex) {
            logger.error("\u274c Error processing Kafka message", ex);
        }
    }

    public ApiResponse processSingleMessage(KafkaMessage message) {
        try {
            List<BatchFile> batchFiles = message.getBatchFiles();
            if (batchFiles == null || batchFiles.isEmpty()) {
                return new ApiResponse("No batch files found", "error", null);
            }

            List<BatchFile> dataFilesOnly = batchFiles.stream()
                    .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                    .toList();
            if (dataFilesOnly.isEmpty()) return new ApiResponse("No DATA files to process", "error", null);
            message.setBatchFiles(dataFilesOnly);

            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(message.getSourceSystem() + ".csv");
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            writeAndUploadMetadataJson(message, batchDir);

            OTResponse otResponse = callOrchestrationBatchApi("<TOKEN>", message);
            if (otResponse == null || otResponse.getJobId() == null || otResponse.getId() == null) {
                return new ApiResponse("Failed to call OT batch input API", "error", null);
            }

            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) return new ApiResponse("_STDDELIVERYFILE.xml not found", "error", null);

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromXml(xmlFile);
            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());
            List<SummaryProcessedFile> processedFiles = buildAndUploadProcessedFiles(jobDir, accountCustomerMap, message);
            Map<String, String> successMap = new HashMap<>();
            for (SummaryProcessedFile s : processedFiles) {
                successMap.put(s.getAccountNumber(), s.getCustomerId());
            }
            processedFiles.addAll(appendFailureEntries(jobDir, message, successMap));

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            String mobstatTriggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, mobstatTriggerPath);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + batchId + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            return new ApiResponse("Success", "success", new SummaryResponse(payload));
        } catch (Exception ex) {
            logger.error("Failed in processing", ex);
            return new ApiResponse("Processing failed: " + ex.getMessage(), "error", null);
        }
    }

    private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
        List<PrintFile> printFiles = new ArrayList<>();
        Path printFolder = jobDir.resolve("print");
        if (!Files.exists(printFolder)) return printFiles;

        try (Stream<Path> paths = Files.list(printFolder)) {
            paths.filter(Files::isRegularFile).forEach(file -> {
                try {
                    String fileName = file.getFileName().toString();
                    String remotePath = String.format("%s/%s/%s/print/%s",
                            msg.getSourceSystem(), msg.getBatchId(), msg.getUniqueConsumerRef(), fileName);

                    String blobUrl = blobStorageService.uploadFile(file.toFile(), remotePath);
                    printFiles.add(new PrintFile(blobUrl));
                } catch (Exception e) {
                    logger.error("Failed to upload print file: {}", file, e);
                }
            });
        } catch (IOException e) {
            logger.error("Error accessing print folder", e);
        }
        return printFiles;
    }

    package com.nedbank.kafka.filemanage.service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.storage.blob.*;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.exception.CustomAppException;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    private final RestTemplate restTemplate;

    @Value("${azure.keyvault.url}")
    private String keyVaultUrl;

    @Value("${azure.blob.storage.format}")
    private String azureStorageFormat;

    @Value("${azure.keyvault.accountKey}")
    private String fmAccountKey;

    @Value("${azure.keyvault.accountName}")
    private String fmAccountName;

    @Value("${azure.keyvault.containerName}")
    private String fmContainerName;

    private String accountKey;
    private String accountName;
    private String containerName;

    public BlobStorageService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) return;

        try {
            logger.info("🔐 Fetching secrets from Azure Key Vault...");
            SecretClient secretClient = new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();

            accountKey = getSecret(secretClient, fmAccountKey);
            accountName = getSecret(secretClient, fmAccountName);
            containerName = getSecret(secretClient, fmContainerName);

            if (accountKey == null || accountName == null || containerName == null) {
                throw new CustomAppException("Secrets missing from Key Vault", 400, HttpStatus.BAD_REQUEST);
            }

            logger.info("✅ Secrets fetched successfully from Key Vault.");
        } catch (Exception e) {
            logger.error("❌ Failed to initialize secrets: {}", e.getMessage(), e);
            throw new CustomAppException("Key Vault integration failure", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String getSecret(SecretClient client, String secretName) {
        try {
            return client.getSecret(secretName).getValue();
        } catch (Exception e) {
            logger.error("❌ Failed to fetch secret '{}': {}", secretName, e.getMessage(), e);
            throw new CustomAppException("Failed to fetch secret: " + secretName, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    // ✅ Upload TEXT content
    public String uploadFile(String content, String targetPath) {
        try {
            initSecrets();
            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(containerName).getBlobClient(targetPath);
            blob.upload(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), content.length(), true);

            logger.info("📤 Uploaded TEXT file to '{}'", blob.getBlobUrl());
            return blob.getBlobUrl();
        } catch (Exception e) {
            logger.error("❌ Upload failed: {}", e.getMessage(), e);
            throw new CustomAppException("Upload failed", 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    // ✅ Upload FILE content (e.g. PDFs, HTML, binary)
    public String uploadFile(Path filePath, String targetPath) {
        try {
            initSecrets();
            byte[] data = Files.readAllBytes(filePath);

            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(containerName).getBlobClient(targetPath);
            blob.upload(new ByteArrayInputStream(data), data.length, true);

            logger.info("📤 Uploaded FILE to '{}'", blob.getBlobUrl());
            return blob.getBlobUrl();
        } catch (Exception e) {
            logger.error("❌ File Upload failed: {}", e.getMessage(), e);
            throw new CustomAppException("File Upload failed", 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String downloadFileContent(String blobPathOrUrl) {
        try {
            initSecrets();
            String container = containerName;
            String blobPath = blobPathOrUrl;

            if (blobPathOrUrl.startsWith("http")) {
                URI uri = new URI(blobPathOrUrl);
                String[] segments = uri.getPath().split("/");
                if (segments.length < 3) throw new CustomAppException("Invalid blob URL", 400, HttpStatus.BAD_REQUEST);
                container = segments[1];
                blobPath = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
            }

            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(container).getBlobClient(blobPath);
            if (!blob.exists()) throw new CustomAppException("Blob not found", 404, HttpStatus.NOT_FOUND);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            blob.download(out);
            return out.toString(StandardCharsets.UTF_8);

        } catch (Exception e) {
            logger.error("❌ Download failed: {}", e.getMessage(), e);
            throw new CustomAppException("Download failed", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String uploadSummaryJson(String filePathOrUrl, KafkaMessage message, String fileName) {
        initSecrets();
        String remotePath = String.format("%s/%s/%s/%s",
                message.getSourceSystem(),
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                fileName);

        try {
            String json = filePathOrUrl.startsWith("http")
                    ? new String(new URL(filePathOrUrl).openStream().readAllBytes(), StandardCharsets.UTF_8)
                    : Files.readString(Paths.get(filePathOrUrl));

            return uploadFile(json, remotePath);
        } catch (Exception e) {
            logger.error("❌ Failed reading summary JSON: {}", e.getMessage(), e);
            throw new CustomAppException("Failed reading summary JSON", 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}
