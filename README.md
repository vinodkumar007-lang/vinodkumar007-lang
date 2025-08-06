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

==
Yes, you're right — disabling endpoint verification isn't safe for production. We've now removed this property from the production setup.


Kafka listener concurrency is now configurable via the kafka.listener.concurrency property (default is 1). The value can be adjusted at runtime based on load, allowing better parallel processing of smaller batches.

We've moved constants, messages, and paths to a common class for easier maintenance.

Field numbers have been replaced with meaningful constants.

Concurrency improvement options are under review for future scalability.

Currently, we've moved the hardcoded values to a common constants class to avoid repetition and make future maintenance easier.

This property is a problem for production , can we fix the endpoint verification?

props.put("ssl.endpoint.identification.algorithm", ""); // Disable hostname verification
====================================
package com.nedbank.kafka.filemanage.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * KafkaConsumerConfig sets up a secure Kafka consumer configuration for the application.
 * This configuration uses SSL for encrypted communication with the Kafka cluster and
 * ensures manual acknowledgment with single-threaded message consumption.
 */
@Configuration
public class KafkaConsumerConfig {

    // Kafka bootstrap server address (comma-separated if multiple)
    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    // Kafka consumer group ID
    @Value("${kafka.consumer.group.id}")
    private String consumerGroupId;

    // Determines behavior when no offset is found ("earliest", "latest", etc.)
    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    // Whether Kafka should auto-commit offsets or not (typically "false")
    @Value("${kafka.consumer.enable.auto.commit}")
    private String enableAutoCommit;

    // Fully qualified class name of key deserializer (e.g., StringDeserializer)
    @Value("${kafka.consumer.key.deserializer}")
    private String keyDeserializer;

    // Fully qualified class name of value deserializer
    @Value("${kafka.consumer.value.deserializer}")
    private String valueDeserializer;

    // Security protocol (e.g., "SSL")
    @Value("${kafka.consumer.security.protocol}")
    private String securityProtocol;

    // SSL truststore path
    @Value("${kafka.consumer.ssl.truststore.location}")
    private String truststoreLocation;

    // SSL truststore password
    @Value("${kafka.consumer.ssl.truststore.password}")
    private String truststorePassword;

    // SSL keystore path
    @Value("${kafka.consumer.ssl.keystore.location}")
    private String keystoreLocation;

    // SSL keystore password
    @Value("${kafka.consumer.ssl.keystore.password}")
    private String keystorePassword;

    // SSL private key password
    @Value("${kafka.consumer.ssl.key.password}")
    private String keyPassword;

    // SSL protocol (e.g., TLSv1.2)
    @Value("${kafka.consumer.ssl.protocol}")
    private String sslProtocol;

    // ✅ NEW: Configurable concurrency level (default is 1 if not provided)
    @Value("${kafka.listener.concurrency:1}")
    private Integer listenerConcurrency;

    /**
     * Configures the Kafka listener container factory.
     * - Enables manual acknowledgment
     * - Sets up SSL-based security
     * - Creates a single-threaded Kafka consumer (concurrency = 1)
     *
     * @return ConcurrentKafkaListenerContainerFactory configured with SSL and manual ack mode
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

        // SSL Security Configuration
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);
        props.put("ssl.endpoint.identification.algorithm", ""); // Disable hostname verification

        // Offset settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // ✅ Use configurable concurrency
        factory.setConcurrency(listenerConcurrency);

        return factory;
    }
}

✅ UNIT TEST DOCUMENT – FileManager Service
📁 Module Scope:
This document covers the unit test strategy for the following Java classes:

KafkaListenerService

BlobStorageService

SummaryJsonWriter

🧪 TEST DOCUMENT STRUCTURE:
Each class section contains:

Target method(s)

Purpose

Dependencies

Mocking Strategy

Test Cases (positive, negative, edge cases)

Exception handling scenarios

1. 🧠 KafkaListenerService
📌 Method: onKafkaMessage(String rawMessage, Acknowledgment ack)
✅ Purpose:
Process a Kafka message end-to-end: parse, copy files, write summary, upload to Blob, send Kafka response.

🔗 Dependencies:
ObjectMapper

BlobStorageService

SummaryJsonWriter

KafkaProducer<String, String>

KafkaConsumer<String, String>

Acknowledgment

🧪 Test Cases:
Test Case ID	Description
KAFKA_001	Valid Kafka message processed end-to-end: file copied, summary written, Kafka message sent
KAFKA_002	Kafka message contains duplicate batchId → message skipped
KAFKA_003	Kafka message with missing fields → ensure no NullPointerException
KAFKA_004	KafkaConsumer fails to poll → log error, ack not called
KAFKA_005	Kafka message with empty batchFiles → log warning, skip processing
KAFKA_006	Exception during upload to blob → assert proper log & ack not called
KAFKA_007	SummaryJsonWriter throws error → validate exception logging
KAFKA_008	Producer.send throws exception → log error but don't crash
KAFKA_009	All dependencies mocked to simulate perfect flow → assert expected log/ack/send

🔁 Mocking Strategy:
Mock BlobStorageService.uploadFileAndReturnLocation

Mock SummaryJsonWriter.appendToSummaryJson

Mock KafkaProducer.send

Mock ObjectMapper.readValue

Mock KafkaConsumer.poll

2. 💾 BlobStorageService
📌 Method: uploadFileAndReturnLocation(...)
✅ Purpose:
Upload a file to Azure Blob Storage and return its URL.

🔗 Dependencies:
BlobServiceClient

BlobContainerClient

BlobClient

🧪 Test Cases:
Test Case ID	Description
BLOB_001	File uploaded successfully, return correct blob URL
BLOB_002	Directory creation missing, createNewBlobPath logic verified
BLOB_003	Upload fails with exception (e.g., IOException) → assert proper error logged
BLOB_004	Input stream is null or corrupted → assert upload failure
BLOB_005	Uploads to correct container and path structure (assert path logic)
BLOB_006	Overwriting same file → ensure correct handling
BLOB_007	MultipartFile or path input edge cases

🔁 Mocking Strategy:
Mock BlobServiceClient.getBlobContainerClient

Mock BlobContainerClient.getBlobClient

Mock BlobClient.upload

Mock BlobClient.getBlobUrl

3. 📝 SummaryJsonWriter
📌 Method: appendToSummaryJson(...)
✅ Purpose:
Append summary metadata to a summary.json file and persist updated JSON.

🔗 Dependencies:
ObjectMapper

java.io.File

File writing streams

🧪 Test Cases:
Test Case ID	Description
SUMMARY_001	Summary.json is written with valid data
SUMMARY_002	Summary file does not exist → create new one
SUMMARY_003	Summary already has existing batch → validate append/merge logic
SUMMARY_004	File write fails (IOException) → assert error handling
SUMMARY_005	Input SummaryPayload is null or empty → handle safely
SUMMARY_006	Validate correct structure: header, metadata, processedFiles, printFiles written
SUMMARY_007	Confirm proper JSON structure with ObjectMapper
SUMMARY_008	Corrupted existing summary.json → handle gracefully

🔁 Mocking Strategy:
Use TemporaryFolder rule for file creation

Mock ObjectMapper.readValue, ObjectMapper.writeValue

Mock file I/O

📦 Test Configuration
Item	Value
Framework	JUnit 5 (preferred) / JUnit 4 (if legacy)
Mocking	Mockito
Assertions	AssertJ / Hamcrest / JUnit
Tools	@Mock, @InjectMocks, @BeforeEach, @Test
Build Tool	Maven / Gradle
Optional	Spring Boot Test for context loading

✅ UNIT TEST DOCUMENT (Overview)
🔹1. KafkaListenerService
Method	Test Case Description	Dependencies to Mock	Edge Cases	Expected Outcome
onKafkaMessage	Valid message: parse, write summary, upload blob, send Kafka msg	objectMapper, blobStorageService, kafkaProducerService, summaryJsonWriter	Invalid JSON, null fields, duplicate batch	Summary written, files uploaded, Kafka ACK sent
processSingleMessage	End-to-end flow from Kafka message to blob URL	All mentioned above	Already processed batch, missing files	Final response with blob URLs, file details
getNextUnprocessedMessage	Polls Kafka and skips already processed	kafkaConsumer	No messages, all already processed	Returns null or valid message
parseKafkaMessage	Deserialize message from JSON	objectMapper	Malformed JSON	Correct KafkaMessage object or throws
buildResponse	Build API response from input	—	Empty fields	Response matches expected format

🔹2. BlobStorageService
Method	Test Case Description	Dependencies to Mock	Edge Cases	Expected Outcome
uploadFileAndReturnLocation	Uploads byte[] and returns blob URL	BlobServiceClient	Empty content, invalid folderName	Correct path format returned
copyFileFromUrlToBlob	Downloads from URL and re-uploads	downloadFileContent, blob APIs	Bad URL, empty response	File copied, URL returned
downloadFileContent	Downloads content from remote blob URL	HTTP client	Timeout, 404, null stream	Correct byte[] returned or throws

🔹3. SummaryJsonWriter
Method	Test Case Description	Dependencies to Mock	Edge Cases	Expected Outcome
appendToSummaryJson	Appends processedFiles & printFiles to summary file	None	Duplicate entry, null list	File written successfully
writeSummaryJson	Writes full summary.json with all fields	None	Large payload, special chars	File is written correctly and readable


✅ 1. KafkaListenerServiceTest.java
java
Copy
Edit
package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.kafka.support.Acknowledgment;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaListenerServiceTest {

    @InjectMocks
    private KafkaListenerService kafkaListenerService;

    @Mock
    private BlobStorageService blobStorageService;

    @Mock
    private SummaryJsonWriter summaryJsonWriter;

    @Mock
    private Producer<String, String> kafkaProducer;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private Acknowledgment acknowledgment;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testOnKafkaMessage_SuccessfullyProcessesMessage() throws Exception {
        // Arrange
        String rawMessage = "{ \"batchId\": \"123\", \"batchFiles\": [...] }";
        KafkaMessage mockMessage = new KafkaMessage();
        mockMessage.setBatchId("123");
        mockMessage.setBatchFiles(Collections.emptyList());

        when(objectMapper.readValue(eq(rawMessage), eq(KafkaMessage.class))).thenReturn(mockMessage);

        // Act
        kafkaListenerService.onKafkaMessage(rawMessage, acknowledgment);

        // Assert
        verify(summaryJsonWriter, atLeastOnce()).appendToSummaryJson(any(), any(), any(), any());
        verify(blobStorageService, atLeastOnce()).uploadSummaryJson(any(), any());
        verify(kafkaProducer, atLeastOnce()).send(any(ProducerRecord.class));
        verify(acknowledgment).acknowledge();
    }

    @Test
    void testOnKafkaMessage_WhenExceptionThrown_ShouldStillAcknowledge() throws Exception {
        // Arrange
        String rawMessage = "invalid_json";

        when(objectMapper.readValue(eq(rawMessage), eq(KafkaMessage.class))).thenThrow(new RuntimeException("Parse error"));

        // Act
        kafkaListenerService.onKafkaMessage(rawMessage, acknowledgment);

        // Assert
        verify(acknowledgment).acknowledge();
    }
}
✅ 2. BlobStorageServiceTest.java
java
Copy
Edit
package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BlobStorageServiceTest {

    @InjectMocks
    private BlobStorageService blobStorageService;

    @Mock
    private BlobServiceClient blobServiceClient;

    @Mock
    private BlobContainerClient containerClient;

    @Mock
    private BlobClient blobClient;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testUploadSummaryJson_ShouldUploadSuccessfully() {
        // Arrange
        byte[] content = "sample".getBytes();
        String container = "test-container";
        String fileName = "summary.json";
        InputStream stream = new ByteArrayInputStream(content);

        when(blobServiceClient.getBlobContainerClient(container)).thenReturn(containerClient);
        when(containerClient.getBlobClient(fileName)).thenReturn(blobClient);

        // Act
        String result = blobStorageService.uploadSummaryJson(content, fileName);

        // Assert
        verify(blobClient).upload(any(InputStream.class), eq((long) content.length), eq(true));
        assertTrue(result.contains(fileName));
    }
}
✅ 3. SummaryJsonWriterTest.java
java
Copy
Edit
package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.io.File;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SummaryJsonWriterTest {

    @InjectMocks
    private SummaryJsonWriter summaryJsonWriter;

    @Mock
    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testAppendToSummaryJson_ShouldWriteSuccessfully() throws Exception {
        // Arrange
        SummaryPayload payload = new SummaryPayload();
        String path = "output/test_summary.json";
        File file = new File(path);
        file.getParentFile().mkdirs();
        file.createNewFile();

        // Act
        summaryJsonWriter.appendToSummaryJson(path, payload, "file", "batch123");

        // Assert
        assertTrue(new File(path).exists());
    }
}
