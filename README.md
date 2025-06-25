package com.nedbank.kafka.filemanage.service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;

@Slf4j
@Service
public class KafkaListenerService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        log.info("✅ KafkaListenerService initialized.");
    }

    @KafkaListener(topics = "${kafka.input.topic}", groupId = "${kafka.consumer.group.id}")
    public void listen(String message) {
        log.info("📥 Kafka message received:\n{}", message);
        try {
            JsonNode root = objectMapper.readTree(message);

            String blobUrl = root.path("blobUrl").asText();
            String fileName = root.path("fileName").asText();
            String batchId = root.path("batchId").asText();
            String guiRefId = root.path("guiRefId").asText();

            // Fallback if guiRefId is null or blank
            if (guiRefId == null || guiRefId.isBlank()) {
                guiRefId = java.util.UUID.randomUUID().toString();
            }

            // Final mount path: /mnt/nfs/dev-exstream/dev-SA/job/{batchId}/{guiRefId}/{fileName}
            Path mountPath = Paths.get("/mnt/nfs/dev-exstream/dev-SA/job", batchId, guiRefId, fileName);
            Files.createDirectories(mountPath.getParent());

            downloadBlobToPath(blobUrl, mountPath);

            log.info("✅ File placed at: {}", mountPath);

        } catch (Exception e) {
            log.error("❌ Error while processing Kafka message", e);
        }
    }

    private void downloadBlobToPath(String blobUrl, Path targetPath) throws IOException {
        log.info("⬇️  Downloading blob: {}", blobUrl);

        BlobClient blobClient = new BlobClientBuilder()
                .endpoint(getBlobEndpoint(blobUrl))
                .sasToken(getSasToken(blobUrl))
                .containerName(getContainerName(blobUrl))
                .blobName(getBlobName(blobUrl))
                .buildClient();

        BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();

        try (var inputStream = blockBlobClient.openInputStream()) {
            Files.copy(inputStream, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }

        log.info("📂 Blob file successfully written to: {}", targetPath);
    }

    // Helpers to parse blob URL
    private String getBlobEndpoint(String blobUrl) {
        return blobUrl.substring(0, blobUrl.indexOf(".core.windows.net") + 17);
    }

    private String getContainerName(String blobUrl) {
        String afterNet = blobUrl.split(".net/")[1];
        return afterNet.substring(0, afterNet.indexOf('/'));
    }

    private String getBlobName(String blobUrl) {
        String afterNet = blobUrl.split(".net/")[1];
        return afterNet.substring(afterNet.indexOf('/') + 1, blobUrl.contains("?") ? blobUrl.indexOf('?') : blobUrl.length());
    }

    private String getSasToken(String blobUrl) {
        return blobUrl.contains("?") ? blobUrl.substring(blobUrl.indexOf("?") + 1) : "";
    }
}
