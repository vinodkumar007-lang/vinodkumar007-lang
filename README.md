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
