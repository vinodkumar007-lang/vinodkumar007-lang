package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.ApiResponse;
import com.nedbank.kafka.filemanage.model.BatchFile;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    /*@Value("${mount.path.base}")
    private String mountPathBase;  // e.g. /mnt/nfs/dev-exstream/dev-SA/job

    @Value("${opentext.api.url}")
    private String opentextApiUrl;*/

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate(); // to call OpenText API

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
            try {
                String blobUrl = file.getBlobUrl();
                String fileName = extractFileName(blobUrl);

                // === Build mount path like: /mnt/nfs/.../batchId/guiRef/file.csv
                String localMountPath = String.format("%s/%s/%s", "/mnt/nfs/dev-exstream/dev-SA/jobs", batchId, guiRef);
                File mountDir = new File(localMountPath);
                if (!mountDir.exists()) mountDir.mkdirs();

                String targetFilePath = localMountPath + "/" + fileName;

                // === Download blob file content and write to mount
                String content = blobStorageService.downloadFileContent(blobUrl);
                Files.write(Paths.get(targetFilePath), content.getBytes(StandardCharsets.UTF_8));

                logger.info("📁 Saved DATA file to mount: {}", targetFilePath);

                // === Replace blobUrl with mount path
                file.setBlobUrl(targetFilePath);

            } catch (Exception ex) {
                logger.error("❌ Failed to download and mount blob file: {}", file.getBlobUrl(), ex);
                return new ApiResponse("Failed to mount file: " + file.getBlobUrl(), "error", null);
            }
        }

        try {
            // === Send updated KafkaMessage to OpenText as metadata.json
            String updatedJson = objectMapper.writeValueAsString(message);
            logger.info("📤 Sending metadata.json to OpenText API at: {}", "https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService");

            restTemplate.postForEntity("https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService", updatedJson, String.class);

            return new ApiResponse("Sent metadata to OT", "success", null);
        } catch (Exception e) {
            logger.error("❌ Failed to send metadata.json to OT", e);
            return new ApiResponse("Failed to call OT API", "error", null);
        }
    }

    private String extractFileName(String blobUrl) {
        if (blobUrl == null || blobUrl.isEmpty()) return "unknown.csv";
        String[] segments = blobUrl.split("/");
        return segments.length > 0 ? segments[segments.length - 1] : "unknown.csv";
    }
}
