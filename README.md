package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.ApiResponse;
import com.nedbank.kafka.filemanage.model.BatchFile;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    // Hardcoded temporarily for testing
    private static final String MOUNT_PATH_BASE = "/mnt/nfs/dev-exstream/dev-SA/jobs";
    private static final String OPENTEXT_API_URL = "https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate(); // for OpenText API

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeKafkaMessage(String message) {
        try {
            logger.info("📩 Received Kafka message.");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
            ApiResponse response = processSingleMessage(kafkaMessage);
            logger.info("✅ Kafka message processed and sent to OT: {}", response.getMessage());
        } catch (Exception ex) {
            logger.error("❌ Error processing Kafka message", ex);
        }
    }

    private ApiResponse processSingleMessage(KafkaMessage message) throws UnsupportedEncodingException {
        if (message == null || message.getBatchFiles() == null || message.getBatchFiles().isEmpty()) {
            return new ApiResponse("Empty or invalid message", "error", null);
        }

        List<BatchFile> validFiles = message.getBatchFiles().stream()
                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                .toList();

        if (validFiles.isEmpty()) {
            return new ApiResponse("No DATA files found", "error", null);
        }

        String batchId = message.getBatchId();
        String guiRef = message.getUniqueConsumerRef();

        for (BatchFile file : validFiles) {
            String blobUrl = file.getBlobUrl();
            try {
                String fileName = extractFileName(blobUrl);
                Path localMountPath = Path.of(MOUNT_PATH_BASE, batchId, guiRef);
                Files.createDirectories(localMountPath); // safer directory creation

                Path targetFilePath = localMountPath.resolve(fileName);
                String content = blobStorageService.downloadFileContent(blobUrl);
                Files.write(targetFilePath, content.getBytes(StandardCharsets.UTF_8));

                logger.info("📁 Saved DATA file to mount: {}", targetFilePath);

                // Replace blobUrl with mount path
                file.setBlobUrl(targetFilePath.toString());

            } catch (Exception ex) {
                logger.error("❌ Failed to mount blob file [batchId={}, guiRef={}, url={}]", batchId, guiRef, blobUrl, ex);
                return new ApiResponse("Failed to mount file: " + blobUrl, "error", null);
            }
        }

        try {
            String updatedJson = objectMapper.writeValueAsString(message);
            logger.info("📤 Sending metadata.json to OpenText API at: {}", OPENTEXT_API_URL);

            restTemplate.postForEntity(OPENTEXT_API_URL, updatedJson, String.class);

            return new ApiResponse("Sent metadata to OT", "success", null);
        } catch (Exception e) {
            logger.error("❌ Failed to send metadata.json to OT [batchId={}, guiRef={}]", message.getBatchId(), message.getUniqueConsumerRef(), e);
            return new ApiResponse("Failed to call OT API", "error", null);
        }
    }

    private String extractFileName(String blobUrl) {
        if (blobUrl == null || blobUrl.isEmpty()) return "unknown.csv";
        String[] segments = blobUrl.split("/");
        return segments.length > 0 ? segments[segments.length - 1] : "unknown.csv";
    }
}
