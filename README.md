public String uploadFile(File file, String targetPath) {
    try {
        initSecrets();
        BlobServiceClient blobClient = new BlobServiceClientBuilder()
                .endpoint(String.format(azureStorageFormat, accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobClient blob = blobClient.getBlobContainerClient(containerName).getBlobClient(targetPath);
        try (InputStream inputStream = new FileInputStream(file)) {
            blob.upload(inputStream, file.length(), true);
        }

        logger.info("Uploaded binary file to '{}'", blob.getBlobUrl());
        return blob.getBlobUrl();
    } catch (Exception e) {
        logger.error("Upload failed for binary file: {}", e.getMessage(), e);
        throw new CustomAppException("Binary upload failed", 605, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
