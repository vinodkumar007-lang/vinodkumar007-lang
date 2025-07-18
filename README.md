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

    public String uploadFile(File file, String targetPath) {
        try {
            initSecrets();
            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(containerName).getBlobClient(targetPath);
            try (InputStream inputStream = new FileInputStream(file)) {
                blob.upload(inputStream, file.length(), true);
            }

            logger.info("Uploaded binary file to '{}'", blob.getBlobUrl());
            return blob.getBlobUrl();
        } catch (Exception e) {
            logger.error("Upload failed for binary file: {}", e.getMessage(), e);
            throw new CustomAppException("Binary upload failed", 605, HttpStatus.INTERNAL_SERVER_ERROR, e);
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

===================

package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

@Component
public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static String writeSummaryJsonToFile(SummaryPayload payload) {
        if (payload == null) {
            logger.error("SummaryPayload is null. Cannot write summary.json.");
            throw new IllegalArgumentException("SummaryPayload cannot be null");
        }

        try {
            String batchId = Optional.ofNullable(payload.getBatchID()).orElse("unknown");
            String fileName = "summary_" + batchId + ".json";

            Path tempDir = Files.createTempDirectory("summaryFiles");
            Path summaryFilePath = tempDir.resolve(fileName);

            File summaryFile = summaryFilePath.toFile();
            if (summaryFile.exists()) {
                Files.delete(summaryFilePath);
                logger.warn("Existing summary file deleted: {}", summaryFilePath);
            }

            objectMapper.writeValue(summaryFile, payload);
            logger.info("✅ Summary JSON written at: {}", summaryFilePath);

            return summaryFilePath.toAbsolutePath().toString();

        } catch (Exception e) {
            logger.error("❌ Failed to write summary.json", e);
            throw new RuntimeException("Failed to write summary JSON", e);
        }
    }

    public static SummaryPayload buildPayload(KafkaMessage message,
                                              List<SummaryProcessedFile> processedFiles,
                                              int pagesProcessed,
                                              List<PrintFile> printFiles,
                                              String mobstatTriggerPath,
                                              int customersProcessed) {

        SummaryPayload payload = new SummaryPayload();

        payload.setBatchID(message.getBatchId());
        payload.setFileName(message.getBatchId() + ".csv");
        payload.setMobstatTriggerFile(mobstatTriggerPath);

        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());
        header.setTimestamp(Instant.now().toString());
        payload.setHeader(header);

        String overallStatus = "Completed";
        if (processedFiles != null && !processedFiles.isEmpty()) {
            boolean allFailed = processedFiles.stream().allMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()));
            boolean anyFailed = processedFiles.stream().anyMatch(f ->
                    "FAILURE".equalsIgnoreCase(f.getStatusCode()) || "PARTIAL".equalsIgnoreCase(f.getStatusCode()));
            if (allFailed) overallStatus = "Failure";
            else if (anyFailed) overallStatus = "Partial";
        }

        Metadata metadata = new Metadata();
        metadata.setTotalFilesProcessed(customersProcessed);
        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        payload.setMetadata(metadata);

        Payload payloadDetails = new Payload();
        payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
        payloadDetails.setRunPriority(message.getRunPriority());
        payloadDetails.setEventID(message.getEventID());
        payloadDetails.setEventType(message.getEventType());
        payloadDetails.setRestartKey(message.getRestartKey());
        payloadDetails.setFileCount(pagesProcessed);
        payload.setPayload(payloadDetails);

        List<CustomerSummary> customerSummaries = buildCustomerSummaries(processedFiles);
        payload.setCustomerSummaries(customerSummaries);

        // Optional: include printFiles if needed
        payload.setPrintFiles(printFiles);

        return payload;
    }

    private static List<CustomerSummary> buildCustomerSummaries(List<SummaryProcessedFile> processedFiles) {
        List<CustomerSummary> resultList = new ArrayList<>();

        Map<String, Map<String, List<SummaryProcessedFile>>> grouped = new HashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            if (file.getCustomerId() == null || file.getAccountNumber() == null) continue;

            grouped
                    .computeIfAbsent(file.getCustomerId(), k -> new HashMap<>())
                    .computeIfAbsent(file.getAccountNumber(), k -> new ArrayList<>())
                    .add(file);
        }

        for (Map.Entry<String, Map<String, List<SummaryProcessedFile>>> customerEntry : grouped.entrySet()) {
            String customerId = customerEntry.getKey();
            Map<String, List<SummaryProcessedFile>> accountMap = customerEntry.getValue();

            CustomerSummary customerSummary = new CustomerSummary();
            customerSummary.setCustomerId(customerId);

            int totalAccounts = 0;
            int totalSuccess = 0;
            int totalFailures = 0;

            for (Map.Entry<String, List<SummaryProcessedFile>> accountEntry : accountMap.entrySet()) {
                String accountNumber = accountEntry.getKey();
                List<SummaryProcessedFile> files = accountEntry.getValue();

                AccountSummary acc = new AccountSummary();
                acc.setAccountNumber(accountNumber);

                for (SummaryProcessedFile file : files) {
                    String method = file.getOutputMethod(); // ✅ fixed
                    String status = file.getStatus();
                    String url = file.getBlobURL();

                    if ("EMAIL".equalsIgnoreCase(method)) {
                        acc.setPdfEmailStatus(status);
                        acc.setPdfEmailBlobUrl(url);
                    } else if ("ARCHIVE".equalsIgnoreCase(method)) {
                        acc.setPdfArchiveStatus(status);
                        acc.setPdfArchiveBlobUrl(url);
                    } else if ("MOBSTAT".equalsIgnoreCase(method)) {
                        acc.setPdfMobstatStatus(status);
                        acc.setPdfMobstatBlobUrl(url);
                    } else if ("PRINT".equalsIgnoreCase(method)) {
                        acc.setPrintStatus(status);
                        acc.setPrintBlobUrl(url);
                    } else {
                        logger.warn("❗ Unrecognized output method: {}", method);
                    }

                    if ("success".equalsIgnoreCase(status)) totalSuccess++;
                    else totalFailures++;
                }

                customerSummary.getAccounts().add(acc);
                totalAccounts++;
            }

            customerSummary.setTotalAccounts(totalAccounts);
            customerSummary.setTotalSuccess(totalSuccess);
            customerSummary.setTotalFailure(totalFailures);

            resultList.add(customerSummary);
        }

        return resultList;
    }
}
