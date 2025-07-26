@KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
    try {
        logger.info("\uD83D\uDCE5 Received Kafka message: {}", rawMessage);
        KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
        String batchId = message.getBatchId();
        Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
        Files.createDirectories(batchDir);
        logger.info("\uD83D\uDCC1 Created input directory: {}", batchDir);

        for (BatchFile file : message.getBatchFiles()) {
            String blobUrl = file.getBlobUrl();
            Path localPath = batchDir.resolve(file.getFilename());

            // ✅ Stream-based download — NO memory load
            blobStorageService.downloadFileToLocal(blobUrl, localPath);

            file.setBlobUrl(localPath.toString());
            logger.info("⬇️ Downloaded file {} to local path {}", blobUrl, localPath);
        }

        // Optional: ack.acknowledge(); if using manual commit

    } catch (Exception e) {
        logger.error("❌ Error processing Kafka message: {}", e.getMessage(), e);
    }
}

=======

public Path downloadFileToLocal(String blobUrl, Path localFilePath) {
    try {
        initSecrets();
        String container = containerName;
        String blobPath = blobUrl;

        if (blobUrl.startsWith("http")) {
            URI uri = new URI(blobUrl);
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

        try (OutputStream outputStream = new FileOutputStream(localFilePath.toFile())) {
            blob.download(outputStream); // ✅ Streaming, no memory load
        }

        return localFilePath;

    } catch (Exception e) {
        logger.error("❌ Download to local failed: {}", e.getMessage(), e);
        throw new CustomAppException("Download failed", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
