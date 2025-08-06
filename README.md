package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class responsible for:
 * - Building the SummaryPayload object from processed data
 * - Writing the summary JSON file to a local temporary directory
 * - Decoding and organizing final print file URLs
 * - Calculating metadata such as total customers processed, file count, and overall status
 */
@Component
public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    /**
     * Writes a SummaryPayload object as a formatted JSON file to a temp directory.
     *
     * @param payload The SummaryPayload object to be serialized
     * @return Absolute path to the written JSON file
     */
    public static String writeSummaryJsonToFile(SummaryPayload payload) {
        if (payload == null) {
            logger.error("SummaryPayload is null. Cannot write summary.json.");
            throw new IllegalArgumentException("SummaryPayload cannot be null");
        }

        try {
            String batchId = Optional.ofNullable(payload.getBatchID()).orElse("unknown");
            String fileName = "summary_" + batchId + ".json";

            // Create temp dir and resolve full path
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

    /**
     * Constructs a SummaryPayload object from various input values.
     *
     * @param kafkaMessage Kafka input message object
     * @param processedList List of processed file entries
     * @param fileName Output summary file name
     * @param batchId Batch identifier
     * @param timestamp Timestamp string
     * @param errorMap Map of errors keyed by account and delivery method
     * @param printFiles List of print file URLs
     * @return SummaryPayload object
     */
    public static SummaryPayload buildPayload(
            KafkaMessage kafkaMessage,
            List<SummaryProcessedFile> processedList,
            String fileName,
            String batchId,
            String timestamp,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> printFiles
    ) {
        SummaryPayload payload = new SummaryPayload();
        payload.setBatchID(batchId);
        payload.setFileName(fileName);
        payload.setTimestamp(timestamp);

        // Populate header metadata
        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        // Build processed file entries from summary list
        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap, printFiles);
        payload.setProcessedFileList(processedFileEntries);

        // Count successful file URLs for final payload
        int totalFileUrls = (int) processedFileEntries.stream()
                .flatMap(entry -> Stream.of(
                        new AbstractMap.SimpleEntry<>(entry.getEmailBlobUrl(), entry.getEmailStatus()),
                        new AbstractMap.SimpleEntry<>(entry.getPrintBlobUrl(), entry.getPrintStatus()),
                        new AbstractMap.SimpleEntry<>(entry.getMobstatBlobUrl(), entry.getMobstatStatus()),
                        new AbstractMap.SimpleEntry<>(entry.getArchiveBlobUrl(), entry.getArchiveStatus())
                ))
                .filter(e -> e.getKey() != null && !e.getKey().trim().isEmpty()
                        && "SUCCESS".equalsIgnoreCase(e.getValue()))
                .count();

        // Populate payload details from KafkaMessage
        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
        payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
        payloadInfo.setEventID(kafkaMessage.getEventID());
        payloadInfo.setEventType(kafkaMessage.getEventType());
        payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
        payloadInfo.setFileCount(totalFileUrls);
        payload.setPayload(payloadInfo);

        // Metadata: count distinct customers and determine final status
        Metadata metadata = new Metadata();
        metadata.setTotalCustomersProcessed((int) processedFileEntries.stream()
                .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
                .distinct()
                .count());

        Set<String> statuses = processedFileEntries.stream()
                .map(ProcessedFileEntry::getOverallStatus)
                .collect(Collectors.toSet());

        String overallStatus;
        if (statuses.size() == 1) {
            overallStatus = statuses.iterator().next();
        } else if (statuses.contains("SUCCESS") && statuses.contains("FAILED")) {
            overallStatus = "PARTIAL";
        } else if (statuses.contains("PARTIAL") || statuses.size() > 1) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "FAILED";
        }

        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        // ✅ Step 1: Assign status based on conditions before decoding
        for (PrintFile pf : printFiles) {
            String psUrl = pf.getPrintFileURL();

            if (psUrl != null && psUrl.endsWith(".ps")) {
                // .ps file exists, set SUCCESS
                pf.setPrintStatus("SUCCESS");
            } else if (psUrl != null && errorMap.containsKey(psUrl)) {
                // Error found for this file
                pf.setPrintStatus("FAILED");
            } else {
                // Not found or unclear
                pf.setPrintStatus("");
            }
        }

// ✅ Step 2: Decode and collect into final printFileList
        List<PrintFile> printFileList = new ArrayList<>();

        for (PrintFile pf : printFiles) {
            if (pf.getPrintFileURL() != null) {
                String decodedUrl = URLDecoder.decode(pf.getPrintFileURL(), StandardCharsets.UTF_8);

                PrintFile printFile = new PrintFile();
                printFile.setPrintFileURL(decodedUrl);
                printFile.setPrintStatus(pf.getPrintStatus() != null ? pf.getPrintStatus() : "");

                printFileList.add(printFile);
            }
        }

// ✅ Step 3: Set in payload
        payload.setPrintFiles(printFileList);

        return payload;
    }

    /**
     * Groups SummaryProcessedFile list into ProcessedFileEntry list by customer/account,
     * maps delivery statuses, and assigns overall status.
     *
     * @param processedFiles List of files that were processed
     * @param errorMap Map of errors for each delivery method
     * @return List of grouped ProcessedFileEntry objects with status and blob URLs
     */
    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> printFiles) {

        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();

            ProcessedFileEntry entry = grouped.computeIfAbsent(key, k -> {
                ProcessedFileEntry newEntry = new ProcessedFileEntry();
                newEntry.setCustomerId(file.getCustomerId());
                newEntry.setAccountNumber(file.getAccountNumber());
                return newEntry;
            });

            String outputType = file.getOutputType();
            String blobUrl = file.getBlobUrl();
            Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

            // Determine delivery status
            String status;
            if (isNonEmpty(blobUrl)) {
                status = "SUCCESS";
            } else if ("FAILED".equalsIgnoreCase(errors.getOrDefault(outputType, ""))) {
                status = "FAILED";
            } else {
                status = "";
            }

            switch (outputType) {
                case "EMAIL" -> {
                    entry.setEmailBlobUrl(blobUrl);
                    entry.setEmailStatus(status);
                }
                case "PRINT" -> {
                    if (printFiles != null && !printFiles.isEmpty()) {
                        entry.setPrintStatus("SUCCESS");
                    } else {
                        entry.setPrintStatus("");
                    }
                }
                case "MOBSTAT" -> {
                    entry.setMobstatBlobUrl(blobUrl);
                    entry.setMobstatStatus(status);
                }
                case "ARCHIVE" -> {
                    entry.setArchiveBlobUrl(blobUrl);
                    entry.setArchiveStatus(status);
                }
            }
        }

        for (ProcessedFileEntry entry : grouped.values()) {
            String email = entry.getEmailStatus();
            String print = entry.getPrintStatus();
            String mobstat = entry.getMobstatStatus();
            String archive = entry.getArchiveStatus();

            boolean isEmailSuccess = "SUCCESS".equals(email);
            boolean isPrintSuccess = "SUCCESS".equals(print);
            boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
            boolean isArchiveSuccess = "SUCCESS".equals(archive);

            boolean isEmailMissingOrFailed = email == null || "FAILED".equals(email) || "".equals(email);
            boolean isPrintMissingOrFailed = print == null || "FAILED".equals(print) || "".equals(print);
            boolean isMobstatMissingOrFailed = mobstat == null || "FAILED".equals(mobstat) || "".equals(mobstat);

            // Default status logic
            if (isEmailSuccess && isArchiveSuccess) {
                entry.setOverallStatus("SUCCESS");
            } else if (isMobstatSuccess && isArchiveSuccess && isEmailMissingOrFailed && isPrintMissingOrFailed) {
                entry.setOverallStatus("SUCCESS");
            } else if (isPrintSuccess && isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed) {
                entry.setOverallStatus("SUCCESS");
            } else if (isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed && isPrintMissingOrFailed) {
                entry.setOverallStatus("PARTIAL");
            } else if (isArchiveSuccess) {
                entry.setOverallStatus("PARTIAL");
            } else {
                entry.setOverallStatus("FAILED");
            }

            // ✅ Final override: mark as PARTIAL if account has any error (regardless of requested method)
            if (errorMap.containsKey(entry.getAccountNumber())
                    && !"FAILED".equals(entry.getOverallStatus())) {
                entry.setOverallStatus("PARTIAL");
            }
        }

        return new ArrayList<>(grouped.values());
    }
    /**
     * Utility method to check if a string is non-null and non-blank.
     *
     * @param value The input string
     * @return true if not null or blank
     */
    private static boolean isNonEmpty(String value) {
        return value != null && !value.trim().isEmpty();
    }
}

================
package com.nedbank.kafka.filemanage.service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.constants.BlobStorageConstants;
import com.nedbank.kafka.filemanage.exception.CustomAppException;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.Arrays;

/**
 * Service for interacting with Azure Blob Storage.
 * Handles uploading and downloading files and content,
 * fetching secrets from Azure Key Vault, and constructing blob paths based on KafkaMessage metadata.
 *
 * Uploads include text, binary, file path-based, and summary JSONs.
 * Downloads stream content directly to local file system to minimize memory usage.
 *
 * Dependencies: Azure Blob Storage SDK, Azure Key Vault SDK.
 */
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

    private Instant lastSecretRefreshTime = null;
    private static final long SECRET_CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes

    public BlobStorageService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * Lazily initializes and caches secrets required for Azure Blob operations.
     *
     * <p>This method ensures that secrets such as {@code accountName}, {@code accountKey}, and
     * {@code containerName} are fetched from Azure Key Vault only when needed.</p>
     *
     * <p>To avoid repeated Key Vault calls, the secrets are cached and refreshed based on a
     * configurable TTL (time-to-live). If the TTL has not expired since the last refresh, the
     * cached values are reused.</p>
     *
     * <p>This design balances performance with sensitivity to secret updates in Key Vault.</p>
     */
    private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) {
            if (lastSecretRefreshTime != null &&
                    Instant.now().toEpochMilli() - lastSecretRefreshTime.toEpochMilli() < BlobStorageConstants.SECRET_CACHE_TTL_MS) {
                return;
            }
        }

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
                throw new CustomAppException(BlobStorageConstants.ERR_MISSING_SECRETS, 400, HttpStatus.BAD_REQUEST);
            }

            lastSecretRefreshTime = Instant.now();
            logger.info("✅ Secrets fetched successfully from Key Vault.");
        } catch (Exception e) {
            logger.error("❌ Failed to initialize secrets: {}", e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_KV_FAILURE, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    @Scheduled(fixedDelay = BlobStorageConstants.SECRET_CACHE_TTL_MS)
    public void scheduledSecretRefresh() {
        logger.info("🕒 Scheduled refresh of secrets...");
        initSecrets();
    }

    /**
     * Fetches a specific secret from Azure Key Vault.
     *
     * @param client     The initialized SecretClient.
     * @param secretName The name of the secret to retrieve.
     * @return The secret value.
     */
    private String getSecret(SecretClient client, String secretName) {
        try {
            return client.getSecret(secretName).getValue();
        } catch (Exception e) {
            logger.error("❌ Failed to fetch secret '{}': {}", secretName, e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_FETCH_SECRET + secretName, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    /**
     * Unified method to upload String, byte[], File, or Path to Azure Blob Storage.
     */
    public String uploadFile(Object input, String targetPath) {
        try {
            byte[] content;
            String fileName = Paths.get(targetPath).getFileName().toString();

            if (input instanceof String str) {
                content = str.getBytes(StandardCharsets.UTF_8);
            } else if (input instanceof byte[] bytes) {
                content = bytes;
            } else if (input instanceof File file) {
                content = Files.readAllBytes(file.toPath());
            } else if (input instanceof Path path) {
                content = Files.readAllBytes(path);
            } else {
                throw new IllegalArgumentException(BlobStorageConstants.ERR_UNSUPPORTED_TYPE + input.getClass());
            }

            return uploadToBlobStorage(content, targetPath, fileName);
        } catch (IOException e) {
            logger.error("❌ Failed to read input for upload: {}", e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_PROCESS_UPLOAD, 606, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String uploadToBlobStorage(byte[] content, String targetPath, String fileName) {
        try {
            initSecrets();
            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(containerName).getBlobClient(targetPath);

            BlobHttpHeaders headers = new BlobHttpHeaders()
                    .setContentType(resolveMimeType(fileName, content));

            blob.upload(new ByteArrayInputStream(content), content.length, true);
            blob.setHttpHeaders(headers);

            logger.info("📤 Uploaded file to '{}'", blob.getBlobUrl());
            return blob.getBlobUrl();
        } catch (Exception e) {
            logger.error("❌ Upload failed: {}", e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_UPLOAD_FAIL, 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String resolveMimeType(String fileName, byte[] content) {
        try (InputStream is = new ByteArrayInputStream(content)) {
            Tika tika = new Tika();
            String mimeType = tika.detect(is, fileName);
            return mimeType != null ? mimeType : BlobStorageConstants.DEFAULT_MIME;
        } catch (IOException e) {
            logger.warn("⚠️ Tika failed to detect MIME type. Defaulting. Error: {}", e.getMessage());
            return BlobStorageConstants.DEFAULT_MIME;
        }
    }

    /**
     * Uploads a file based on KafkaMessage context to a constructed blob path.
     *
     * @param file       The file to upload.
     * @param folderName The target subfolder in blob.
     * @param msg        The Kafka message for metadata.
     * @return The uploaded blob URL.
     */
    public String uploadFileByMessage(File file, String folderName, KafkaMessage msg) {
        try {
            byte[] content = Files.readAllBytes(file.toPath());
            String targetPath = buildBlobPath(file.getName(), folderName, msg);
            return uploadFile(content, targetPath);
        } catch (IOException e) {
            logger.error("❌ Error reading file for upload: {}", file.getAbsolutePath(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_FILE_READ, 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    /**
     * Constructs a blob path using file name and KafkaMessage fields.
     *
     * @param fileName   The name of the file.
     * @param folderName Folder name to be included in path.
     * @param msg        Kafka message with metadata.
     * @return Formatted blob path.
     */
    private String buildBlobPath(String fileName, String folderName, KafkaMessage msg) {
        String sourceSystem = sanitize(msg.getSourceSystem(), BlobStorageConstants.FALLBACK_SOURCE);
        String consumerRef = sanitize(msg.getUniqueConsumerRef(), BlobStorageConstants.FALLBACK_CONSUMER);

        return sourceSystem + "/" +
                consumerRef + "/" +
                folderName + "/" +
                fileName;
    }

    private String sanitize(String value, String fallback) {
        if (value == null || value.trim().isEmpty()) return fallback;
        return value.replaceAll(BlobStorageConstants.SANITIZE_REGEX, BlobStorageConstants.SANITIZE_REPLACEMENT);
    }

    /**
     * Downloads a file from blob storage to a local path using streaming.
     *
     * @param blobUrl       Blob URL of the file.
     * @param localFilePath Local path where file will be stored.
     * @return Path to the downloaded file.
     */
    public Path downloadFileToLocal(String blobUrl, Path localFilePath) {
        try {
            initSecrets();
            String container = containerName;
            String blobPath = blobUrl;

            if (blobUrl.startsWith("http")) {
                URI uri = new URI(blobUrl);
                String[] segments = uri.getPath().split("/");
                if (segments.length < 3)
                    throw new CustomAppException(BlobStorageConstants.ERR_INVALID_URL, 400, HttpStatus.BAD_REQUEST);
                container = segments[1];
                blobPath = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
            }

            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(container).getBlobClient(blobPath);
            if (!blob.exists())
                throw new CustomAppException(BlobStorageConstants.ERR_BLOB_NOT_FOUND, 404, HttpStatus.NOT_FOUND);

            try (OutputStream outputStream = new FileOutputStream(localFilePath.toFile())) {
                blob.download(outputStream);
            }

            return localFilePath;

        } catch (Exception e) {
            logger.error("❌ Download to local failed: {}", e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_DOWNLOAD_FAIL, 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    /**
     * Uploads summary.json from either a file path or URL into a target blob path.
     *
     * @param filePathOrUrl File path or URL of summary JSON.
     * @param message       KafkaMessage used to construct remote blob path.
     * @param fileName      Target file name in blob.
     * @return Uploaded blob URL.
     */
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
            throw new CustomAppException(BlobStorageConstants.ERR_SUMMARY_JSON, 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}

=====================================
package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.constants.AppConstants;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.*;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
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
import java.util.concurrent.*;
import java.util.stream.*;
import static com.nedbank.kafka.filemanage.constants.AppConstants.*;
/**
 * KafkaListenerService is responsible for:
 * - Listening to Kafka input topic for file batch processing messages
 * - Downloading blob files to local mount path
 * - Triggering Orchestration APIs (OT) like Debtman/MFC
 * - Waiting for generated output (STD XML), parsing error report and customer summaries
 * - Uploading processed files and generating summary.json
 * - Publishing final response message to Kafka output topic
 *
 * This service acts as an orchestrator between Kafka, Blob Storage, OT system, and summary file generation.
 */
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

    @Value("${ot.service.mfc.url}")
    private String orchestrationMfcUrl;

    @Value("${ot.auth.token}")
    private String orchestrationAuthToken;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    public KafkaListenerService(BlobStorageService blobStorageService, KafkaTemplate<String, String> kafkaTemplate) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
    }
    /**
     * Kafka consumer method to handle messages from input topic.
     * Performs validation on message structure, downloads files,
     * and triggers orchestration API.
     *
     * @param rawMessage Raw Kafka message in JSON string format
     * @param ack        Kafka acknowledgment to commit offset manually
     */
    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        String batchId = "";
        try {
            logger.info("📩 [batchId: unknown] Received Kafka message: {}", rawMessage);
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            batchId = message.getBatchId();
            List<BatchFile> batchFiles = message.getBatchFiles();
            if (batchFiles == null || batchFiles.isEmpty()) {
                logger.error("❌ [batchId: {}] Rejected - Empty BatchFiles", batchId);
                ack.acknowledge();
                return;
            }

            long dataCount = batchFiles.stream()
                    .filter(f -> FILE_TYPE_DATA.equalsIgnoreCase(f.getFileType()))
                    .count();
            long refCount = batchFiles.stream()
                    .filter(f -> FILE_TYPE_REF.equalsIgnoreCase(f.getFileType()))
                    .count();

            if (dataCount == 1 && refCount == 0) {
                logger.info("✅ [batchId: {}] Valid with 1 DATA file", batchId);
            } else if (dataCount > 1) {
                logger.error("❌ [batchId: {}] Rejected - Multiple DATA files", batchId);
                ack.acknowledge();
                return;
            } else if (dataCount == 0 && refCount > 0) {
                logger.error("❌ [batchId: {}] Rejected - Only REF files", batchId);
                ack.acknowledge();
                return;
            } else if (dataCount == 1 && refCount > 0) {
                logger.info("✅ [batchId: {}] Valid with DATA + REF files (both will be passed to OT)", batchId);
                message.setBatchFiles(batchFiles);
            } else {
                logger.error("❌ [batchId: {}] Rejected - Invalid or unsupported file type combination", batchId);
                ack.acknowledge();
                return;
            }

            String sanitizedBatchId = batchId.replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);
            String sanitizedSourceSystem = message.getSourceSystem().replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);

            Path batchDir = Paths.get(mountPath, INPUT_FOLDER, sanitizedSourceSystem, sanitizedBatchId);
            if (Files.exists(batchDir)) {
                logger.warn("⚠️ [batchId: {}] Directory already exists at path: {}", batchId, batchDir);
                try (Stream<Path> files = Files.walk(batchDir)) {
                    files.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                    logger.info("🧹 [batchId: {}] Cleaned existing input directory: {}", batchId, batchDir);
                } catch (IOException e) {
                    logger.error("❌ [batchId: {}] Failed to clean directory {} - {}", batchId, batchDir, e.getMessage(), e);
                    throw e;
                }
            }

            Files.createDirectories(batchDir);
            logger.info("📁 [batchId: {}] Created input directory: {}", batchId, batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                Path localPath = batchDir.resolve(file.getFilename());

                try {
                    if (Files.exists(localPath)) {
                        logger.warn("♻️ [batchId: {}] File already exists, overwriting: {}", batchId, localPath);
                        Files.delete(localPath);
                    }

                    blobStorageService.downloadFileToLocal(blobUrl, localPath);

                    if (!Files.exists(localPath)) {
                        logger.error("❌ [batchId: {}] File missing after download: {}", batchId, localPath);
                        throw new IOException("Download failed for: " + localPath);
                    }

                    file.setBlobUrl(localPath.toString());
                    logger.info("⬇️ [batchId: {}] Downloaded file: {} to {}", batchId, blobUrl, localPath);
                } catch (Exception e) {
                    logger.error("❌ [batchId: {}] Failed to download or overwrite file: {} - {}", batchId, blobUrl, e.getMessage(), e);
                    throw e;
                }
            }

            String sourceSystem = message.getSourceSystem();

            if (SOURCE_DEBTMAN.equalsIgnoreCase(sourceSystem) && (otOrchestrationApiUrl == null || otOrchestrationApiUrl.isBlank())) {
                logger.error("❌ [batchId: {}] otOrchestrationApiUrl is not configured for 'DEBTMAN'", batchId);
                throw new IllegalArgumentException(ERROR_DEBTMAN_URL_NOT_CONFIGURED);
            }

            if (SOURCE_MFC.equalsIgnoreCase(sourceSystem) && (orchestrationMfcUrl == null || orchestrationMfcUrl.isBlank())) {
                logger.error("❌ [batchId: {}] orchestrationMfcUrl is not configured for 'MFC'", batchId);
                throw new IllegalArgumentException(ERROR_MFC_URL_NOT_CONFIGURED);
            }

            String url = switch (sourceSystem.toUpperCase()) {
                case SOURCE_DEBTMAN -> otOrchestrationApiUrl;
                case SOURCE_MFC -> orchestrationMfcUrl;
                default -> {
                    logger.error("❌ [batchId: {}] Unsupported source system '{}'", batchId, sourceSystem);
                    throw new IllegalArgumentException("Unsupported source system: " + sourceSystem);
                }
            };

            if (url == null || url.isBlank()) {
                logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sourceSystem);
                ack.acknowledge();
                return;
            }

            // ✅ Acknowledge before async OT call
            ack.acknowledge();

            String finalBatchId = batchId;
            executor.submit(() -> {
                try {
                    logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", finalBatchId, url);
                    OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);
                    logger.info("📤 [batchId: {}] OT request sent successfully", finalBatchId);
                    processAfterOT(message, otResponse);
                } catch (Exception ex) {
                    logger.error("❌ [batchId: {}] Error during async OT or post-processing: {}", finalBatchId, ex.getMessage(), ex);
                }
            });

        } catch (Exception ex) {
            logger.error("❌ [batchId: {}] Kafka message processing failed. Error: {}", batchId, ex.getMessage(), ex);
            ack.acknowledge();
        }
    }

    /**
     * Parses the STD XML file to extract customer delivery status information.
     *
     * @param xmlFile  STD Delivery XML file
     * @param errorMap ErrorReport map to determine status (SUCCESS/PARTIAL/FAILED)
     * @return List of CustomerSummary objects
     */
    private List<CustomerSummary> parseSTDXml(File xmlFile, Map<String, Map<String, String>> errorMap) {
        List<CustomerSummary> customerSummaries = new ArrayList<>();

        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document document = builder.parse(xmlFile);
            document.getDocumentElement().normalize();

            NodeList customerNodes = document.getElementsByTagName("customer");

            for (int i = 0; i < customerNodes.getLength(); i++) {
                Element customerElement = (Element) customerNodes.item(i);

                String accountNumber = null;
                String cisNumber = null;
                List<String> deliveryMethods = new ArrayList<>();

                NodeList keyNodes = customerElement.getElementsByTagName("key");
                for (int j = 0; j < keyNodes.getLength(); j++) {
                    Element keyElement = (Element) keyNodes.item(j);
                    String keyName = keyElement.getAttribute("name");

                    if ("AccountNumber".equalsIgnoreCase(keyName)) {
                        accountNumber = keyElement.getTextContent();
                    } else if ("CISNumber".equalsIgnoreCase(keyName)) {
                        cisNumber = keyElement.getTextContent();
                    }
                }

                NodeList queueNodes = customerElement.getElementsByTagName("queueName");
                for (int q = 0; q < queueNodes.getLength(); q++) {
                    String method = queueNodes.item(q).getTextContent().trim().toUpperCase();
                    if (!method.isEmpty()) {
                        deliveryMethods.add(method);
                    }
                }

                if (accountNumber != null && cisNumber != null) {
                    CustomerSummary summary = new CustomerSummary();
                    summary.setAccountNumber(accountNumber);
                    summary.setCisNumber(cisNumber);
                    summary.setCustomerId(accountNumber);

                    Map<String, String> deliveryStatusMap = errorMap.getOrDefault(accountNumber, new HashMap<>());
                    summary.setDeliveryStatus(deliveryStatusMap);

                    long failedCount = deliveryMethods.stream()
                            .filter(method -> "FAILED".equalsIgnoreCase(deliveryStatusMap.getOrDefault(method, "")))
                            .count();

                    if (failedCount == deliveryMethods.size()) {
                        summary.setStatus("FAILED");
                    } else if (failedCount > 0) {
                        summary.setStatus("PARTIAL");
                    } else {
                        summary.setStatus("SUCCESS");
                    }

                    customerSummaries.add(summary);

                    logger.debug("📋 Customer: {}, CIS: {}, Methods: {}, Failed: {}, FinalStatus: {}",
                            accountNumber, cisNumber, deliveryMethods, failedCount, summary.getStatus());
                }
            }

        } catch (Exception e) {
            logger.error("❌ Failed parsing STD XML file: {}", xmlFile.getAbsolutePath(), e);
            throw new RuntimeException("Failed to parse XML file: " + xmlFile.getName(), e);
        }

        return customerSummaries;
    }

    /**
     * Performs all post-orchestration processing like:
     * - Waiting for generated STD XML
     * - Parsing error report and STD XML
     * - Uploading output files and building processed files list
     * - Writing and uploading summary.json
     * - Sending Kafka output with summary response
     *
     * @param message     Kafka input message object
     * @param otResponse  OT job response containing jobId and id
     */
    private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        String batchId = message.getBatchId(); // golden thread
        try {
            logger.info("[{}] ⏳ Waiting for XML for jobId={}, id={}", batchId, otResponse.getJobId(), otResponse.getId());
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");

            logger.info("[{}] ✅ Found XML file: {}", batchId, xmlFile);

            Map<String, Map<String, String>> errorMap = parseErrorReport(message);
            logger.info("[{}] 🧾 Parsed error report with {} entries", batchId, errorMap.size());

            List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
            logger.info("[{}] 📊 Total customerSummaries parsed: {}", batchId, customerSummaries.size());

            List<SummaryProcessedFile> customerList = customerSummaries.stream()
                    .map(cs -> {
                        SummaryProcessedFile spf = new SummaryProcessedFile();
                        spf.setAccountNumber(cs.getAccountNumber());
                        spf.setCustomerId(cs.getCisNumber());
                        return spf;
                    })
                    .collect(Collectors.toList());

            Path jobDir = Paths.get(mountPath, AppConstants.OUTPUT_FOLDER, message.getSourceSystem(), otResponse.getJobId());

            List<SummaryProcessedFile> processedFiles =
                    buildDetailedProcessedFiles(jobDir, customerList, errorMap, message);
            logger.info("[{}] 📦 Processed {} customer records", batchId, processedFiles.size());

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("[{}] 🖨️ Uploaded {} print files", batchId, printFiles.size());

            String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
            logger.info("[{}] 📱 Found Mobstat URL: {}", batchId, mobstatTriggerUrl);

            Map<String, Integer> summaryCounts = extractSummaryCountsFromXml(xmlFile);

            String allFileNames = message.getBatchFiles().stream()
                    .map(BatchFile::getFilename)
                    .collect(Collectors.joining(", "));

            SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message, processedFiles, allFileNames, batchId,
                    String.valueOf(message.getTimestamp()), errorMap, printFiles
            );

            if (payload.getHeader() != null) {
                payload.getHeader().setTimestamp(String.valueOf(message.getTimestamp()));
            }

            String fileName = AppConstants.SUMMARY_FILENAME_PREFIX + batchId + AppConstants.JSON_EXTENSION;
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName);
            payload.setSummaryFileURL(decodeUrl(summaryUrl));
            logger.info("[{}] 📁 Summary JSON uploaded to: {}", batchId, decodeUrl(summaryUrl));

            logger.info("[{}] 📄 Final Summary Payload:\n{}", batchId,
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(batchId);
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setPayload(payload.getPayload());
            response.setSummaryFileURL(decodeUrl(summaryUrl));
            response.setTimestamp(String.valueOf(message.getTimestamp()));

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                    new ApiResponse("Summary generated", "COMPLETED", response)));

            logger.info("[{}] ✅ Kafka output sent with response: {}", batchId,
                    objectMapper.writeValueAsString(response));

        } catch (Exception e) {
            logger.error("[{}] ❌ Error post-OT summary generation: {}", batchId, e.getMessage(), e);
        }
    }

    /**
     * Looks for a `.trigger` file in the output job directory and uploads it if found.
     *
     * @param jobDir  Path to the job output directory
     * @param message Kafka input message
     * @return Blob URL of uploaded trigger file or null if not found
     */
    private String findAndUploadMobstatTriggerFile(Path jobDir, KafkaMessage message) {
        try (Stream<Path> stream = Files.list(jobDir)) {
            Optional<Path> trigger = stream.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(TRIGGER_FILE_EXTENSION))
                    .findFirst();

            if (trigger.isPresent()) {
                Path triggerFile = trigger.get();
                String blobUrl = blobStorageService.uploadFile(
                        triggerFile.toFile(),
                        String.format(MOBSTAT_TRIGGER_UPLOAD_PATH_FORMAT,
                                message.getSourceSystem(), message.getBatchId(), triggerFile.getFileName())
                );

                logger.info("📤 Uploaded MOBSTAT trigger file: {} -> {}", triggerFile, blobUrl);
                return decodeUrl(blobUrl);
            } else {
                logger.warn("⚠️ No .trigger file found in MOBSTAT job directory: {}", jobDir);
                return null;
            }

        } catch (IOException e) {
            logger.error("⚠️ Failed to scan for .trigger file in jobDir: {}", jobDir, e);
            return null;
        }
    }

    /**
     * Invokes the external OT orchestration batch API.
     *
     * @param token Authentication token
     * @param url   API endpoint
     * @param msg   Kafka input message payload
     * @return OTResponse containing jobId and id
     */
    private OTResponse callOrchestrationBatchApi(String token, String url, KafkaMessage msg) {
        OTResponse otResponse = new OTResponse();
        try {
            logger.info("📡 Initiating OT orchestration call to URL: {} for batchId: {} and sourceSystem: {}",
                    url, msg.getBatchId(), msg.getSourceSystem());

            HttpHeaders headers = new HttpHeaders();
            headers.set(AppConstants.HEADER_AUTHORIZATION, AppConstants.BEARER_PREFIX + token);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            logger.debug("📨 OT Request Payload: {}", objectMapper.writeValueAsString(msg));

            ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);
            logger.info("✅ Received OT response with status: {} for batchId: {}",
                    response.getStatusCode(), msg.getBatchId());

            List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get(AppConstants.OT_RESPONSE_DATA_KEY);
            if (data != null && !data.isEmpty()) {
                Map<String, Object> item = data.get(0);
                otResponse.setJobId((String) item.get(AppConstants.OT_JOB_ID_KEY));
                otResponse.setId((String) item.get(AppConstants.OT_ID_KEY));
                msg.setJobName(otResponse.getJobId());
                otResponse.setSuccess(true);

                logger.info("🎯 OT Job created successfully - JobID: {}, ID: {}, BatchID: {}",
                        otResponse.getJobId(), otResponse.getId(), msg.getBatchId());
            } else {
                logger.error("❌ No data found in OT orchestration response for batchId: {}", msg.getBatchId());
                otResponse.setSuccess(false);
                otResponse.setMessage(AppConstants.NO_OT_DATA_MESSAGE);
            }

            return otResponse;
        } catch (Exception e) {
            logger.error("❌ Exception during OT orchestration call for batchId: {} - {}",
                    msg.getBatchId(), e.getMessage(), e);
            otResponse.setSuccess(false);
            otResponse.setMessage(AppConstants.OT_CALL_FAILURE_PREFIX + e.getMessage());
            return otResponse;
        }
    }

    /**
     * Waits for the STD delivery XML file to be generated in job directory.
     * Ensures the file is stable (not still being written).
     *
     * @param jobId OT job ID
     * @param id    OT sub-job ID
     * @return File object for the found XML or null if timeout
     * @throws InterruptedException If thread sleep is interrupted
     */
    private File waitForXmlFile(String jobId, String id) throws InterruptedException {
        Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, AppConstants.DOCGEN_FOLDER);
        long startTime = System.currentTimeMillis();
        File xmlFile = null;

        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (Files.exists(docgenRoot)) {
                try (Stream<Path> paths = Files.walk(docgenRoot)) {
                    Optional<Path> xmlPath = paths
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().equalsIgnoreCase(AppConstants.XML_FILE_NAME))
                            .findFirst();

                    if (xmlPath.isPresent()) {
                        xmlFile = xmlPath.get().toFile();

                        long size1 = xmlFile.length();
                        TimeUnit.SECONDS.sleep(1);
                        long size2 = xmlFile.length();

                        if (size1 > 0 && size1 == size2) {
                            logger.info(AppConstants.LOG_FOUND_STABLE_XML, xmlFile.getAbsolutePath());
                            return xmlFile;
                        } else {
                            logger.info(AppConstants.LOG_XML_SIZE_CHANGING, xmlFile.getAbsolutePath());
                        }
                    }
                } catch (IOException e) {
                    logger.warn(AppConstants.LOG_ERROR_SCANNING_FOLDER, jobId, id, e.getMessage(), e);
                }
            } else {
                logger.debug(AppConstants.LOG_DOCGEN_FOLDER_NOT_FOUND, jobId, id, docgenRoot);
            }

            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }

        String errMsg = String.format(AppConstants.LOG_XML_TIMEOUT, docgenRoot, jobId, id);
        logger.error(errMsg);
        throw new IllegalStateException(errMsg);
    }

    /**
     * Parses ErrorReport.csv file under the job directory and maps delivery method status
     *
     * @param msg Kafka message to locate job folder
     * @return Map of accountNumber -> (method -> status)
     */
    private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
        Map<String, Map<String, String>> result = new HashMap<>();
        try {
            String jobRootPath = mountPath + "/jobs";
            Path jobRoot = Paths.get(jobRootPath);
            String jobId = msg.getJobName();

            // Find all ErrorReport.csv files under this job's path
            Path jobPath = jobRoot.resolve(jobId);
            if (!Files.exists(jobPath)) {
                logger.warn("❌ Job path not found: {}", jobPath);
                return result;
            }

            logger.info("🔍 Searching ErrorReport.csv under: {}", jobPath);

            try (Stream<Path> stream = Files.walk(jobPath)) {
                Optional<Path> reportFile = stream
                        .filter(path -> path.getFileName().toString().equalsIgnoreCase(AppConstants.ERROR_REPORT_FILE_NAME))
                        .findFirst();

                if (reportFile.isEmpty()) {
                    logger.warn("⚠️ ErrorReport.csv not found under job {}", jobId);
                    return result;
                }

                Path reportPath = reportFile.get();
                logger.info("✅ Found ErrorReport.csv at: {}", reportPath);

                try (BufferedReader reader = Files.newBufferedReader(reportPath)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\\|");

                        if (parts.length >= 3) {
                            String account = parts[AppConstants.ERROR_REPORT_INDEX_ACCOUNT].trim();

                            String method = AppConstants.DEFAULT_METHOD;
                            if (parts.length >= 4) {
                                method = parts[AppConstants.ERROR_REPORT_INDEX_METHOD_V1].trim();
                            } else if (parts.length >= 3) {
                                method = parts[AppConstants.ERROR_REPORT_INDEX_METHOD_V2].trim();
                            }

                            String status = AppConstants.DEFAULT_STATUS;
                            if (parts.length >= 5) {
                                status = parts[AppConstants.ERROR_REPORT_INDEX_STATUS_V1].trim();
                            } else if (parts.length >= 4) {
                                status = parts[AppConstants.ERROR_REPORT_INDEX_STATUS_V2].trim();
                            }

                            if (method.isEmpty()) method = AppConstants.DEFAULT_METHOD;
                            if (status.isEmpty()) status = AppConstants.DEFAULT_STATUS;

                            result.computeIfAbsent(account, k -> new HashMap<>()).put(method.toUpperCase(), status);
                        }
                    }
                } catch (IOException e) {
                    logger.error("❌ Error reading ErrorReport.csv: {}", e.getMessage(), e);
                }
            }
        } catch (Exception ex) {
            logger.error("❌ Failed to parse error report: {}", ex.getMessage(), ex);
        }

        return result;
    }

    /**
     * Builds a list of SummaryProcessedFile entries with archive/output blob URLs
     * and delivery status for each customer (EMAIL, MOBSTAT, PRINT).
     *
     * @param jobDir        Output job directory
     * @param customerList  Customer basic summary list
     * @param errorMap      Error report map (account -> method -> status)
     * @param msg           Kafka message for path info
     * @return List of SummaryProcessedFile objects with blob URLs and status
     */
    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();
        List<String> deliveryFolders = List.of(
                AppConstants.FOLDER_EMAIL,
                AppConstants.FOLDER_MOBSTAT,
                AppConstants.FOLDER_PRINT
        );

        Map<String, String> folderToOutputMethod = Map.of(
                AppConstants.FOLDER_EMAIL, AppConstants.OUTPUT_EMAIL,
                AppConstants.FOLDER_MOBSTAT, AppConstants.OUTPUT_MOBSTAT,
                AppConstants.FOLDER_PRINT, AppConstants.OUTPUT_PRINT
        );

        if (jobDir == null || customerList == null || msg == null) {
            logger.warn("⚠️ One or more input parameters are null: jobDir={}, customerList={}, msg={}",
                    jobDir, customerList, msg);
            return finalList;
        }

        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);

        for (SummaryProcessedFile customer : customerList) {
            if (customer == null) continue;

            String account = customer.getAccountNumber();
            if (account == null || account.isBlank()) {
                logger.warn("⚠️ Skipping customer with empty account number");
                continue;
            }

            // Archive upload
            String archiveBlobUrl = null;
            try {
                if (Files.exists(archivePath)) {
                    Optional<Path> archiveFile = Files.list(archivePath)
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (archiveFile.isPresent()) {
                        archiveBlobUrl = blobStorageService.uploadFileByMessage(
                                archiveFile.get().toFile(), AppConstants.FOLDER_ARCHIVE, msg);

                        SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, archiveEntry);
                        archiveEntry.setOutputType(AppConstants.OUTPUT_ARCHIVE);
                        archiveEntry.setBlobUrl(decodeUrl(archiveBlobUrl));

                        finalList.add(archiveEntry);
                        logger.info("📦 Uploaded archive file for account {}: {}", account, archiveBlobUrl);
                    }
                }
            } catch (Exception e) {
                logger.warn("⚠️ Failed to upload archive file for account {}: {}", account, e.getMessage(), e);
            }

            // EMAIL, MOBSTAT, PRINT uploads
            for (String folder : deliveryFolders) {
                String outputMethod = folderToOutputMethod.get(folder);
                Path methodPath = jobDir.resolve(folder);
                String blobUrl = null;

                try {
                    if (Files.exists(methodPath)) {
                        Optional<Path> match = Files.list(methodPath)
                                .filter(Files::isRegularFile)
                                .filter(p -> p.getFileName().toString().contains(account))
                                .findFirst();

                        if (match.isPresent()) {
                            blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                            logger.info("✅ Uploaded {} file for account {}: {}", outputMethod, account, blobUrl);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("⚠️ Failed to upload {} file for account {}: {}", outputMethod, account, e.getMessage(), e);
                }

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputType(outputMethod);
                entry.setBlobUrl(decodeUrl(blobUrl));

                if (archiveBlobUrl != null) {
                    entry.setArchiveOutputType(AppConstants.OUTPUT_ARCHIVE);
                    entry.setArchiveBlobUrl(archiveBlobUrl);
                }

                finalList.add(entry);
            }
        }

        return finalList;
    }

    /**
     * Uploads all files under the print directory and creates PrintFile entries.
     *
     * @param jobDir Output job directory
     * @param msg    Kafka message for blob path info
     * @return List of PrintFile objects containing blob URLs
     */
    private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
        List<PrintFile> printFiles = new ArrayList<>();

        if (jobDir == null || msg == null || msg.getSourceSystem() == null) {
            logger.error("❌ Invalid input: jobDir={}, msg={}, sourceSystem={}", jobDir, msg, msg != null ? msg.getSourceSystem() : null);
            return printFiles;
        }

        Path printDir = jobDir.resolve(AppConstants.PRINT_FOLDER_NAME);
        if (!Files.exists(printDir)) {
            logger.info("ℹ️ No '{}' directory found in jobDir: {}", AppConstants.PRINT_FOLDER_NAME, jobDir);
            return printFiles;
        }

        try (Stream<Path> stream = Files.list(printDir)) {
            stream.filter(Files::isRegularFile).forEach(f -> {
                try {
                    String fileName = f.getFileName() != null ? f.getFileName().toString() : AppConstants.UNKNOWN_FILE_NAME;
                    String uploadPath = msg.getSourceSystem() + "/" + AppConstants.PRINT_FOLDER_NAME + "/" + fileName;

                    String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                    printFiles.add(new PrintFile(blob));

                    logger.info("📤 Uploaded print file: {} -> {}", fileName, blob);
                } catch (Exception e) {
                    logger.warn("⚠️ Failed to upload print file: {}", f, e);
                }
            });
        } catch (IOException e) {
            logger.error("❌ Failed to list files in '{}' directory: {}", AppConstants.PRINT_FOLDER_NAME, printDir, e);
        }

        return printFiles;
    }

    /**
     * Extracts customer and page count values from STD XML's outputList node.
     *
     * @param xmlFile STD delivery XML
     * @return Map with "customersProcessed" and "pagesProcessed" as keys
     */
    private Map<String, Integer> extractSummaryCountsFromXml(File xmlFile) {
        Map<String, Integer> summaryCounts = new HashMap<>();

        if (xmlFile == null || !xmlFile.exists() || !xmlFile.canRead()) {
            logger.warn("⚠️ Invalid or unreadable XML file: {}", xmlFile);
            return summaryCounts;
        }

        try {
            Document doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList outputListNodes = doc.getElementsByTagName("outputList");
            if (outputListNodes.getLength() > 0) {
                Element outputList = (Element) outputListNodes.item(0);
                String customersProcessed = outputList.getAttribute(AppConstants.CUSTOMERS_PROCESSED_KEY);
                String pagesProcessed = outputList.getAttribute(AppConstants.PAGES_PROCESSED_KEY);

                int custCount = (customersProcessed != null && !customersProcessed.isBlank())
                        ? Integer.parseInt(customersProcessed.trim()) : 0;
                int pageCount = (pagesProcessed != null && !pagesProcessed.isBlank())
                        ? Integer.parseInt(pagesProcessed.trim()) : 0;

                summaryCounts.put(AppConstants.CUSTOMERS_PROCESSED_KEY, custCount);
                summaryCounts.put(AppConstants.PAGES_PROCESSED_KEY, pageCount);

                logger.info("📄 Extracted summary counts from {}: customersProcessed={}, pagesProcessed={}",
                        xmlFile.getName(), custCount, pageCount);
            } else {
                logger.info("ℹ️ No <outputList> found in XML: {}", xmlFile.getName());
            }
        } catch (Exception e) {
            logger.warn("⚠️ Unable to extract summary counts from XML file: {}", xmlFile.getName(), e);
        }

        return summaryCounts;
    }

    /**
     * Decodes a URL-encoded string using UTF-8 charset.
     *
     * @param url Encoded URL
     * @return Decoded URL string
     */
    private String decodeUrl(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return url;
        }
    }
    /**
     * Gracefully shuts down executor service on bean destruction.
     */
    @PreDestroy
    public void shutdownExecutor() {
        logger.info("⚠️ Shutting down executor service");
        executor.shutdown();
    }
    /**
     * Internal class representing the response from OT orchestration call.
     */
    static class OTResponse {
        private String jobId;
        private String id;
        private boolean success;
        private String message;

        public boolean isSuccess() {
            return success;
        }
        public void setSuccess(boolean success) {
            this.success = success;
        }
        public String getMessage() {
            return message;
        }
        public void setMessage(String message) {
            this.message = message;
        }
        public String getJobId() { return jobId; }
        public void setJobId(String jobId) { this.jobId = jobId; }
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
    }
}
