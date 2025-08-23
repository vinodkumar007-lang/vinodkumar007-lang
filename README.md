// ✅ FIX: Ensure customersProcessed matches XML
Integer xmlCust = summaryCounts.get(AppConstants.CUSTOMERS_PROCESSED_KEY);
if (payload.getMetadata() != null) {
    if (xmlCust != null && xmlCust > 0) {
        payload.getMetadata().setCustomersProcessed(xmlCust);
    } else {
        payload.getMetadata().setCustomersProcessed(customerSummaries.size());
    }
}

public String uploadFileByMessage(File file, String folderName, KafkaMessage msg,
                                  String accountNumber, String cisNumber) {
    try {
        byte[] content = Files.readAllBytes(file.toPath());
        String targetPath = buildBlobPath(file.getName(), folderName, msg, accountNumber, cisNumber);
        return uploadFile(content, targetPath);
    } catch (IOException e) {
        logger.error("❌ Error reading file for upload: {}", file.getAbsolutePath(), e);
        throw new CustomAppException(BlobStorageConstants.ERR_FILE_READ, 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
private String buildBlobPath(String fileName, String folderName, KafkaMessage msg,
                             String accountNumber, String cisNumber) {
    String sourceSystem = sanitize(msg.getSourceSystem(), BlobStorageConstants.FALLBACK_SOURCE);
    String consumerRef  = sanitize(msg.getUniqueConsumerRef(), BlobStorageConstants.FALLBACK_CONSUMER);

    // For ARCHIVE → append account + cis to filename to avoid overwrites
    if (AppConstants.FOLDER_ARCHIVE.equalsIgnoreCase(folderName)) {
        String safeAcc = sanitize(accountNumber, "NAACC");
        String safeCis = sanitize(cisNumber, "NACIS");
        fileName = safeAcc + "_" + safeCis + "_" + fileName;
    }

    return sourceSystem + "/" +
           msg.getBatchId() + "/" +
           consumerRef + "/" +
           folderName + "/" +
           fileName;
}


archiveBlobUrl = blobStorageService.uploadFileByMessage(
        archiveFile.get().toFile(),
        AppConstants.FOLDER_ARCHIVE,
        msg,
        customer.getAccountNumber(),
        customer.getCustomerId()   // cisNumber
);
