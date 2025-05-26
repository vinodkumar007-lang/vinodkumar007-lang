public String uploadFileAndReturnLocation(
        String sourceSystem,
        String fileLocation,
        String batchId,
        String objectId,
        String consumerReference,
        String processReference,
        String timestamp) {

    try {
        if (sourceSystem == null || fileLocation == null || batchId == null || objectId == null 
                || consumerReference == null || processReference == null || timestamp == null) {
            throw new CustomAppException("Required parameters missing", 400, HttpStatus.BAD_REQUEST);
        }

        // TODO: Replace with Vault secrets
        String accountKey = ""; // getSecretFromVault("account_key", getVaultToken());
        String accountName = "nsndvextr01"; // getSecretFromVault("account_name", getVaultToken());
        String containerName = "nsnakscontregecm001"; // getSecretFromVault("container_name", getVaultToken());

        // Extract filename from fileLocation (after last slash)
        String sourceFileName = fileLocation.substring(fileLocation.lastIndexOf("/") + 1);

        // Build target blob path:
        // {sourceSystem}/input/{timestamp}/{batchId}/{consumerReference}_{processReference}/{fileName}
        String blobName = String.format("%s/input/%s/%s/%s_%s/%s",
                sourceSystem,
                timestamp,
                batchId,
                consumerReference.replaceAll("[{}]", ""),
                processReference.replaceAll("[{}]", ""),
                sourceFileName);

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                .credential(new StorageSharedKeyCredential(accountName, accountKey))
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient sourceBlobClient = containerClient.getBlobClient(sourceFileName);
        BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

        try (InputStream inputStream = sourceBlobClient.openInputStream()) {
            long size = sourceBlobClient.getProperties().getBlobSize();
            targetBlobClient.upload(inputStream, size, true);
            logger.info("✅ Uploaded '{}' to '{}'", sourceFileName, targetBlobClient.getBlobUrl());
        } catch (BlobStorageException bse) {
            logger.error("❌ Azure Blob Storage error: {}", bse.getMessage());
            throw new CustomAppException("Blob storage operation failed", 453, HttpStatus.BAD_GATEWAY, bse);
        } catch (SocketException se) {
            logger.error("❌ Network error: {}", se.getMessage());
            throw new CustomAppException("Network issue during blob transfer", 420, HttpStatus.GATEWAY_TIMEOUT, se);
        } catch (Exception e) {
            logger.error("❌ Unexpected error during blob transfer: {}", e.getMessage());
            throw new CustomAppException("Unexpected blob error", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }

        // Return blob URL without SAS
        return targetBlobClient.getBlobUrl();

    } catch (CustomAppException cae) {
        throw cae; // rethrow
    } catch (Exception e) {
        logger.error("❌ Generic error in uploadFileAndReturnLocation: {}", e.getMessage());
        throw new CustomAppException("Internal blob error", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
