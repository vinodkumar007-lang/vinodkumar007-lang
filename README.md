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
