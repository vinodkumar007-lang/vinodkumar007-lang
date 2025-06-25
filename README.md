package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

    @KafkaListener(
            topics = "${kafka.topic.input}",
            groupId = "${kafka.consumer.group.id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeKafkaMessage(String message) {
        try {
            logger.info("Received Kafka message...");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);

            ApiResponse response = processSingleMessage(kafkaMessage);

            // ✅ Only send SUCCESS messages to output topic
            if ("success".equalsIgnoreCase(response.getStatus())) {
                kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));
                logger.info("Kafka message processed & sent to output topic. Response: {}", response.getMessage());
            } else {
                logger.warn("Kafka message processing returned error. Not sending to output topic. Response: {}", response.getMessage());
            }

        } catch (Exception ex) {
            logger.error("Error processing Kafka message", ex);
        }
    }

    private ApiResponse processSingleMessage(KafkaMessage message) throws UnsupportedEncodingException {
        if (message == null) {
            return new ApiResponse("Empty message", "error",
                    new SummaryPayloadResponse("Empty message", "error", new SummaryResponse()).getSummaryResponse());
        }

        // === NEW VALIDATION START ===
        List<BatchFile> batchFiles = message.getBatchFiles();

        if (batchFiles == null || batchFiles.isEmpty()) {
            logger.error("BatchFiles is empty or null. Rejecting message.");
            return new ApiResponse("Invalid message: BatchFiles is empty or null", "error",
                    new SummaryPayloadResponse("Invalid message: BatchFiles is empty or null", "error", new SummaryResponse()).getSummaryResponse());
        }

        long dataCount = batchFiles.stream()
                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                .count();

        long refCount = batchFiles.stream()
                .filter(f -> "REF".equalsIgnoreCase(f.getFileType()))
                .count();

        if (dataCount == 0 && refCount > 0) {
            logger.error("Message contains only REF files. Rejecting message.");
            return new ApiResponse("Invalid message: only REF files present", "error",
                    new SummaryPayloadResponse("Invalid message: only REF files present", "error", new SummaryResponse()).getSummaryResponse());
        }

        if (dataCount > 1) {
            logger.error("Message contains multiple DATA files ({}). Rejecting message.", dataCount);
            return new ApiResponse("Invalid message: multiple DATA files present", "error",
                    new SummaryPayloadResponse("Invalid message: multiple DATA files present", "error", new SummaryResponse()).getSummaryResponse());
        }

        // If 1 DATA + REF(s), we process only DATA — so prepare filtered list
        List<BatchFile> validFiles = batchFiles.stream()
                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                .toList();

        logger.info("BatchFiles validation passed. DATA files to process: {}", validFiles.size());
        // === NEW VALIDATION END ===

        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setTimestamp(instantToIsoString(message.getTimestamp()));
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());

        Payload payload = new Payload();
        payload.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payload.setRunPriority(message.getRunPriority());
        payload.setEventType(message.getEventType());

        List<SummaryProcessedFile> processedFiles = new ArrayList<>();
        List<PrintFile> printFiles = new ArrayList<>();
        Metadata metadata = new Metadata();
        String summaryFileUrl;

        String fileName = null;
        if (!validFiles.isEmpty()) {
            String firstBlobUrl = validFiles.get(0).getBlobUrl();
            String blobPath = extractBlobPath(firstBlobUrl);
            fileName = extractFileName(blobPath);
        }
        if (fileName == null || fileName.isEmpty()) {
            fileName = message.getBatchId() + "_summary.json";
        }

        for (BatchFile file : validFiles) {
            try {
                String inputFileContent = blobStorageService.downloadFileContent(extractFileName(extractBlobPath(file.getBlobUrl())));
                List<CustomerData> customers = DataParser.extractCustomerData(inputFileContent);
                if (customers.isEmpty()) continue;

                for (CustomerData customer : customers) {
                    File pdfFile = FileGenerator.generatePdf(customer);
                    File htmlFile = FileGenerator.generateHtml(customer);
                    File txtFile = FileGenerator.generateTxt(customer);
                    File mobstatFile = FileGenerator.generateMobstat(customer);

                    String pdfArchiveUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "archive",
                                    customer.getAccountNumber(), pdfFile.getName())).split("\\?")[0];

                    String pdfEmailUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "email",
                                    customer.getAccountNumber(), pdfFile.getName())).split("\\?")[0];

                    String htmlEmailUrl = blobStorageService.uploadFile(htmlFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "html",
                                    customer.getAccountNumber(), htmlFile.getName())).split("\\?")[0];

                    String txtEmailUrl = blobStorageService.uploadFile(txtFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "txt",
                                    customer.getAccountNumber(), txtFile.getName())).split("\\?")[0];

                    String mobstatUrl = blobStorageService.uploadFile(mobstatFile.getAbsolutePath(),
                            buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), message.getJobName(), "mobstat",
                                    customer.getAccountNumber(), mobstatFile.getName())).split("\\?")[0];

                    SummaryProcessedFile processedFile = new SummaryProcessedFile();
                    processedFile.setCustomerId(customer.getCustomerId());
                    processedFile.setAccountNumber(customer.getAccountNumber());
                    processedFile.setPdfArchiveFileUrl(pdfArchiveUrl);
                    processedFile.setPdfEmailFileUrl(pdfEmailUrl);
                    processedFile.setHtmlEmailFileUrl(htmlEmailUrl);
                    processedFile.setTxtEmailFileUrl(txtEmailUrl);
                    processedFile.setPdfMobstatFileUrl(mobstatUrl);
                    processedFile.setStatusCode("OK");
                    processedFile.setStatusDescription("Success");
                    processedFiles.add(processedFile);
                }
            } catch (Exception ex) {
                logger.error("Error processing file '{}': {}", file.getFilename(), ex.getMessage(), ex);
            }
        }

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL(blobStorageService.buildPrintFileUrl(message));
        printFiles.add(printFile);

        payload.setFileCount(processedFiles.size());

        metadata.setProcessingStatus("Completed");
        metadata.setTotalFilesProcessed(processedFiles.size());
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName(fileName);
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);
        summaryPayload.setPayload(payload);
        summaryPayload.setProcessedFiles(processedFiles);
        summaryPayload.setPrintFiles(printFiles);

        String summaryJsonPath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload);

        String summaryFileName = "summary_" + message.getBatchId() + ".json";
        summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message, summaryFileName);
        String decodedUrl = URLDecoder.decode(summaryFileUrl, StandardCharsets.UTF_8);
        summaryPayload.setSummaryFileURL(decodedUrl);

        SummaryResponse summaryResponse = new SummaryResponse();
        summaryResponse.setBatchID(summaryPayload.getBatchID());
        summaryResponse.setFileName(summaryPayload.getFileName());
        summaryResponse.setHeader(summaryPayload.getHeader());
        summaryResponse.setMetadata(summaryPayload.getMetadata());
        summaryResponse.setPayload(summaryPayload.getPayload());
        summaryResponse.setSummaryFileURL(summaryPayload.getSummaryFileURL());
        summaryResponse.setTimestamp(String.valueOf(Instant.now()));

        SummaryPayloadResponse apiPayload = new SummaryPayloadResponse("Batch processed successfully", "success", summaryResponse);
        return new ApiResponse(apiPayload.getMessage(), apiPayload.getStatus(), apiPayload.getSummaryResponse());
    }

    private String buildBlobPath(String sourceSystem, long timestamp, String batchId,
                                 String uniqueConsumerRef, String jobName, String folder,
                                 String customerAccount, String fileName) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.systemDefault());
        String dateStr = dtf.format(Instant.ofEpochMilli(timestamp));
        return String.format("%s/%s/%s/%s/%s/%s/%s",
                sourceSystem, dateStr, batchId, uniqueConsumerRef, jobName, folder, fileName);
    }

    private String extractBlobPath(String fullUrl) {
        if (fullUrl == null) return "";
        try {
            URI uri = URI.create(fullUrl);
            String path = uri.getPath();
            return path.startsWith("/") ? path.substring(1) : path;
        } catch (Exception e) {
            return fullUrl;
        }
    }

    public String extractFileName(String fullPathOrUrl) {
        if (fullPathOrUrl == null || fullPathOrUrl.isEmpty()) return fullPathOrUrl;
        String trimmed = fullPathOrUrl.replaceAll("/+", "/");
        int lastSlashIndex = trimmed.lastIndexOf('/');
        return lastSlashIndex >= 0 ? trimmed.substring(lastSlashIndex + 1) : trimmed;
    }

    private String instantToIsoString(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).toString();
    }
}

package com.nedbank.kafka.filemanage.service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobItem;
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
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
        if (accountKey != null && accountName != null && containerName != null) {
            return;
        }

        try {
            logger.info("🔐 Fetching secrets from Azure Key Vault...");
            SecretClient secretClient = new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();

            // ✅ Updated secret names from your provided URLs
            accountKey = getSecret(secretClient, fmAccountKey);
            accountName = getSecret(secretClient, fmAccountName);
            containerName = getSecret(secretClient, fmContainerName);

            if (accountKey == null || accountKey.isBlank() ||
                    accountName == null || accountName.isBlank() ||
                    containerName == null || containerName.isBlank()) {
                throw new CustomAppException("One or more secrets are null/empty from Key Vault", 400, HttpStatus.BAD_REQUEST);
            }

            logger.info("✅ Secrets fetched successfully from Azure Key Vault.");
        } catch (Exception e) {
            logger.error("❌ Failed to initialize secrets from Key Vault: {}", e.getMessage(), e);
            throw new CustomAppException("Key Vault integration failure", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String getSecret(SecretClient client, String secretName) {
        try {
            KeyVaultSecret secret = client.getSecret(secretName);
            return secret.getValue();
        } catch (Exception e) {
            logger.error("❌ Failed to fetch secret '{}': {}", secretName, e.getMessage(), e);
            throw new CustomAppException("Failed to fetch secret: " + secretName, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String copyFileFromUrlToBlob(String sourceUrl, String targetBlobPath) {
        try {
            initSecrets();

            URI sourceUri = new URI(sourceUrl);
            String host = sourceUri.getHost();
            String[] hostParts = host.split("\\.");
            String sourceAccountName = hostParts[0];

            String path = sourceUri.getPath();
            String[] pathParts = path.split("/", 3);
            if (pathParts.length < 3) {
                throw new CustomAppException("Invalid source URL path: " + path, 400, HttpStatus.BAD_REQUEST);
            }

            String sourceContainerName = pathParts[1];
            String sourceBlobPath = pathParts[2];

            BlobServiceClient sourceBlobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, sourceAccountName))
                    .credential(new StorageSharedKeyCredential(sourceAccountName, accountKey))
                    .buildClient();

            BlobContainerClient sourceContainerClient = sourceBlobServiceClient.getBlobContainerClient(sourceContainerName);
            BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(sourceBlobPath);

            long blobSize = -1;
            for (BlobItem item : sourceContainerClient.listBlobs()) {
                if (item.getName().equals(sourceBlobPath)) {
                    blobSize = item.getProperties().getContentLength();
                    break;
                }
            }

            if (blobSize <= 0) {
                throw new CustomAppException("Source blob is empty or not found: " + sourceUrl, 404, HttpStatus.NOT_FOUND);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            sourceBlobClient.download(outputStream);
            byte[] sourceBlobBytes = outputStream.toByteArray();

            BlobServiceClient targetBlobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient targetContainerClient = targetBlobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = targetContainerClient.getBlobClient(targetBlobPath);

            targetBlobClient.upload(new ByteArrayInputStream(sourceBlobBytes), sourceBlobBytes.length, true);

            logger.info("✅ Copied '{}' to '{}'", sourceUrl, targetBlobClient.getBlobUrl());

            return targetBlobClient.getBlobUrl();

        } catch (Exception e) {
            logger.error("❌ Error copying file from URL: {}", e.getMessage(), e);
            throw new CustomAppException("Error copying file from URL", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String uploadFile(String content, String targetBlobPath) {
        try {
            initSecrets();

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(targetBlobPath);

            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            blobClient.upload(new ByteArrayInputStream(bytes), bytes.length, true);

            logger.info("✅ Uploaded file to '{}'", blobClient.getBlobUrl());

            return blobClient.getBlobUrl();

        } catch (Exception e) {
            logger.error("❌ Error uploading file: {}", e.getMessage(), e);
            throw new CustomAppException("Error uploading file", 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String downloadFileContent(String blobPathOrUrl) {
        try {
            initSecrets();

            String extractedContainerName = containerName;
            String blobName = blobPathOrUrl;

            if (blobPathOrUrl.startsWith("http")) {
                URI uri = new URI(blobPathOrUrl);
                String[] segments = uri.getPath().split("/");

                if (segments == null || segments.length < 3) {
                    throw new CustomAppException("Invalid blob URL format: " + blobPathOrUrl, 400, HttpStatus.BAD_REQUEST);
                }

                extractedContainerName = segments[1];
                blobName = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
            }

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(extractedContainerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            if (!blobClient.exists()) {
                throw new CustomAppException("Blob not found: " + blobName, 404, HttpStatus.NOT_FOUND);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            blobClient.download(outputStream);

            return outputStream.toString(StandardCharsets.UTF_8.name());

        } catch (Exception e) {
            logger.error("❌ Error downloading blob content for '{}': {}", blobPathOrUrl, e.getMessage(), e);
            throw new CustomAppException("Error downloading blob content", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String buildPrintFileUrl(KafkaMessage message) {
        initSecrets();

        if (message == null || message.getBatchId() == null || message.getSourceSystem() == null ||
                message.getUniqueConsumerRef() == null || message.getJobName() == null) {
            throw new CustomAppException("Invalid Kafka message data for building print file URL", 400, HttpStatus.BAD_REQUEST);
        }

        String baseUrl = String.format(azureStorageFormat, accountName, containerName);

        String dateFolder = Instant.ofEpochMilli(message.getTimestamp())
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        String printFileName = message.getBatchId() + "_printfile.pdf";

        return String.format("%s/%s/%s/%s/%s/%s/print/%s",
                baseUrl,
                message.getSourceSystem(),
                dateFolder,
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                message.getJobName(),
                printFileName);
    }

    public String uploadSummaryJson(String localFilePathOrUrl, KafkaMessage message, String summaryFileName) {
        initSecrets();

        if (message == null || message.getBatchId() == null ||
                message.getSourceSystem() == null || message.getUniqueConsumerRef() == null) {
            throw new CustomAppException("Missing Kafka message metadata for uploading summary JSON", 400, HttpStatus.BAD_REQUEST);
        }

        // Use the provided summaryFileName directly (e.g. "summary_<batchId>.json")
        String remoteBlobPath = String.format("%s/%s/%s/%s",
                message.getSourceSystem(),
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                summaryFileName);

        String jsonContent;
        try {
            if (localFilePathOrUrl.startsWith("http://") || localFilePathOrUrl.startsWith("https://")) {
                jsonContent = downloadContentFromUrl(localFilePathOrUrl);
            } else {
                jsonContent = Files.readString(Paths.get(localFilePathOrUrl));
            }
        } catch (Exception e) {
            logger.error("❌ Error reading summary JSON content: {}", e.getMessage(), e);
            throw new CustomAppException("Error reading summary JSON content", 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }

        return uploadFile(jsonContent, remoteBlobPath);
    }

    private String downloadContentFromUrl(String urlString) throws IOException {
        try (InputStream in = new URL(urlString).openStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
