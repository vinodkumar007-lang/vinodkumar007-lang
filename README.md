 public String copyFileFromUrlToBlob(String sourceUrl, String targetBlobPath) {
        try {
            initSecrets();

            // Parse sourceUrl to get source account, container, blob path
            // Or if source account same as target, just get container and blob path
            // For example, sourceUrl: https://<account>.blob.core.windows.net/<container>/<blobPath>

            URI sourceUri = new URI(sourceUrl);
            String host = sourceUri.getHost(); // e.g. nsndvextr01.blob.core.windows.net
            String[] hostParts = host.split("\\.");
            String sourceAccountName = hostParts[0]; // e.g. nsndvextr01

            String path = sourceUri.getPath(); // e.g. /nsnakscontregecm001/DEBTMAN.csv
            // pathParts[1] = container name, rest is blob path
            String[] pathParts = path.split("/", 3);
            if (pathParts.length < 3) {
                throw new CustomAppException("Invalid source URL path: " + path, 400, HttpStatus.BAD_REQUEST);
            }
            String sourceContainerName = pathParts[1];
            String sourceBlobPath = pathParts[2];

            // Create source BlobClient
            BlobServiceClient sourceBlobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", sourceAccountName))
                    .credential(new StorageSharedKeyCredential(sourceAccountName, accountKey)) // you must have source account key
                    .buildClient();

            BlobContainerClient sourceContainerClient = sourceBlobServiceClient.getBlobContainerClient(sourceContainerName);
            BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(sourceBlobPath);

            // Get input stream from source blob
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            sourceBlobClient.download(outputStream);
            byte[] sourceBlobBytes = outputStream.toByteArray();
            InputStream inputStream = new ByteArrayInputStream(sourceBlobBytes);

            // Target blob client
            BlobServiceClient targetBlobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient targetContainerClient = targetBlobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = targetContainerClient.getBlobClient(targetBlobPath);

            targetBlobClient.upload(inputStream, sourceBlobBytes.length, true);

            logger.info("✅ Copied '{}' to '{}'", sourceUrl, targetBlobClient.getBlobUrl());

            return targetBlobClient.getBlobUrl();

        } catch (Exception e) {
            logger.error("❌ Error copying file from URL: {}", e.getMessage(), e);
            throw new CustomAppException("Error copying file from URL", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String uploadFile(String content, String targetBlobPath) {
        try {
            initSecrets();

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(targetBlobPath);

            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            blobClient.upload(new java.io.ByteArrayInputStream(bytes), bytes.length, true);

            logger.info("✅ Uploaded file to '{}'", blobClient.getBlobUrl());

            return blobClient.getBlobUrl();

        } catch (Exception e) {
            logger.error("❌ Error uploading file: {}", e.getMessage(), e);
            throw new CustomAppException("Error uploading file", 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
