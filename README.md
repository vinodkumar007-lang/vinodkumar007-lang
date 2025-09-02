the format expects 3 parameters not 4

try {
                    String blobUrl = blobStorageService.uploadFile(
                            triggerFile.toFile(),
                            String.format(MOBSTAT_TRIGGER_UPLOAD_PATH_FORMAT,
                                    message.getSourceSystem(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), triggerFile.getFileName())
                    );

                    logger.info("📤 Uploaded MOBSTAT trigger file: {} -> {}", triggerFile, blobUrl);
                    return decodeUrl(blobUrl);
                }

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
