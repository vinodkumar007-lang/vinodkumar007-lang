1️⃣ KafkaListenerServiceTest
java
Copy
Edit
package com.example.filemanager.service;

import com.example.filemanager.model.KafkaMessage;
import com.example.filemanager.service.KafkaListenerService;
import com.example.filemanager.service.FileManagerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaListenerServiceTest {

    @InjectMocks
    private KafkaListenerService kafkaListenerService;

    @Mock
    private FileManagerService fileManagerService;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private Acknowledgment acknowledgment;

    @Test
    void shouldProcessValidKafkaMessage() throws Exception {
        String jsonMessage = """
        {
            "batchId": "BATCH123",
            "sourceSystem": "DEBTMAN",
            "fileType": "PRINT",
            "blobUrl": "https://blob.core.windows.net/input/file1.txt"
        }
        """;

        KafkaMessage message = new KafkaMessage();
        message.setBatchId("BATCH123");
        message.setSourceSystem("DEBTMAN");
        message.setFileType("PRINT");

        when(objectMapper.readValue(jsonMessage, KafkaMessage.class))
                .thenReturn(message);

        kafkaListenerService.onKafkaMessage(jsonMessage, acknowledgment);

        verify(fileManagerService, times(1)).processBatch(eq(message));
        verify(acknowledgment, times(1)).acknowledge();
    }

    @Test
    void shouldHandleInvalidJsonGracefully() throws Exception {
        String invalidJson = "{invalid_json}";

        when(objectMapper.readValue(anyString(), eq(KafkaMessage.class)))
                .thenThrow(new RuntimeException("Parsing error"));

        kafkaListenerService.onKafkaMessage(invalidJson, acknowledgment);

        verify(fileManagerService, never()).processBatch(any());
        verify(acknowledgment, never()).acknowledge();
    }
}
2️⃣ SummaryJsonWriterTest
java
Copy
Edit
package com.example.filemanager.util;

import com.example.filemanager.model.SummaryPayload;
import com.example.filemanager.util.SummaryJsonWriter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SummaryJsonWriterTest {

    @Test
    void shouldWriteValidSummaryJson() throws Exception {
        SummaryPayload payload = new SummaryPayload();
        payload.setBatchId("BATCH123");
        payload.setTimestamp(Instant.now().toString());
        payload.setProcessedFiles(List.of(
                new SummaryPayload.ProcessedFile("file1.pdf", "https://blob/file1.pdf", "SUCCESS")
        ));

        File tempFile = File.createTempFile("summary", ".json");

        SummaryJsonWriter writer = new SummaryJsonWriter();
        writer.writeSummaryJson(tempFile.getAbsolutePath(), payload);

        String jsonContent = Files.readString(tempFile.toPath());
        assertTrue(jsonContent.contains("BATCH123"));
        assertTrue(jsonContent.contains("file1.pdf"));
    }
}
3️⃣ BlobStorageServiceTest
java
Copy
Edit
package com.example.filemanager.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.example.filemanager.service.BlobStorageService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BlobStorageServiceTest {

    @InjectMocks
    private BlobStorageService blobStorageService;

    @Mock
    private BlobServiceClient blobServiceClient;

    @Mock
    private BlobContainerClient containerClient;

    @Mock
    private BlobClient blobClient;

    @Test
    void shouldUploadFileAndReturnUrl() {
        when(blobServiceClient.getBlobContainerClient("archive")).thenReturn(containerClient);
        when(containerClient.getBlobClient("BATCH123/file.pdf")).thenReturn(blobClient);
        when(blobClient.getBlobUrl()).thenReturn("https://blob.core.windows.net/archive/BATCH123/file.pdf");

        String url = blobStorageService.uploadFile(
                "archive",
                "BATCH123/file.pdf",
                new ByteArrayInputStream("dummy".getBytes())
        );

        assertEquals("https://blob.core.windows.net/archive/BATCH123/file.pdf", url);
        verify(blobClient, times(1)).upload(any(), eq(true));
    }
}
4️⃣ OpenTextServiceTest
java
Copy
Edit
package com.example.filemanager.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OpenTextServiceTest {

    @InjectMocks
    private OpenTextService openTextService;

    @Mock
    private RestTemplate restTemplate;

    @Test
    void shouldRenderDocumentSuccessfully() {
        String inputData = "sample data";
        String expectedPdf = "PDF_BYTES";

        when(restTemplate.postForObject(anyString(), any(), eq(String.class)))
                .thenReturn(expectedPdf);

        String result = openTextService.renderDocument(inputData);

        assertEquals(expectedPdf, result);
    }
}
5️⃣ KafkaProducerServiceTest
java
Copy
Edit
package com.example.filemanager.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    @InjectMocks
    private KafkaProducerService kafkaProducerService;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void shouldSendMessageSuccessfully() {
        String topic = "output-topic";
        String payload = "{\"status\":\"SUCCESS\"}";

        kafkaProducerService.sendMessage(topic, payload);

        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }
}
