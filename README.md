public String uploadFileAndGenerateSasUrl(String fileLocation, String batchId, String objectId) {
        try {
            if (fileLocation == null || batchId == null || objectId == null) {
                throw new CustomAppException("Required parameters missing", 400, HttpStatus.BAD_REQUEST);
            }

            // TODO: Replace with Vault secrets
            String accountKey =  ""; //getSecretFromVault("account_key", getVaultToken());
            String accountName = "nsndvextr01"; //getSecretFromVault("account_name", getVaultToken());  //"nsndvextr01";
            String containerName = "nsnakscontregecm001"; //getSecretFromVault("container_name", getVaultToken()); //"nsnakscontregecm001";

            String extension = getFileExtension(fileLocation);
            String blobName = objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

            String sourceBlobName = fileLocation.substring(fileLocation.lastIndexOf("/") + 1);
            BlobClient sourceBlobClient = containerClient.getBlobClient(sourceBlobName);

            try (InputStream inputStream = sourceBlobClient.openInputStream()) {
                long size = sourceBlobClient.getProperties().getBlobSize();
                targetBlobClient.upload(inputStream, size, true);
                logger.info("✅ Uploaded '{}' to '{}'", sourceBlobName, targetBlobClient.getBlobUrl());
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

            try {
                BlobServiceSasSignatureValues sasValues = new BlobServiceSasSignatureValues(
                        OffsetDateTime.now().plusHours(24),
                        new BlobSasPermission().setReadPermission(true)
                );
                String sasToken = targetBlobClient.generateSas(sasValues);
                return targetBlobClient.getBlobUrl() + "?" + sasToken;
            } catch (Exception e) {
                logger.error("❌ SAS token generation failed: {}", e.getMessage());
                throw new CustomAppException("Failed to generate SAS URL", 453, HttpStatus.BAD_GATEWAY, e);
            }

        } catch (CustomAppException cae) {
            throw cae; // rethrow
        } catch (Exception e) {
            logger.error("❌ Generic error in uploadFileAndGenerateSasUrl: {}", e.getMessage());
            throw new CustomAppException("Internal blob error", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
