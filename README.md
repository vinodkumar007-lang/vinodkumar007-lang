// ✅ FIX: Ensure customersProcessed matches XML
Integer xmlCust = summaryCounts.get(AppConstants.CUSTOMERS_PROCESSED_KEY);
if (payload.getMetadata() != null) {
    if (xmlCust != null && xmlCust > 0) {
        payload.getMetadata().setCustomersProcessed(xmlCust);
    } else {
        payload.getMetadata().setCustomersProcessed(customerSummaries.size());
    }
}

private String buildBlobPath(String fileName, String folderName, KafkaMessage msg) {
    String sourceSystem = sanitize(msg.getSourceSystem(), BlobStorageConstants.FALLBACK_SOURCE);
    String consumerRef = sanitize(msg.getUniqueConsumerRef(), BlobStorageConstants.FALLBACK_CONSUMER);

    // Ensure uniqueness per customer by prefixing with accountNumber or cisNumber
    String accountOrCis = (msg.getAccountNumber() != null && !msg.getAccountNumber().isBlank())
            ? msg.getAccountNumber()
            : (msg.getCisNumber() != null && !msg.getCisNumber().isBlank()
                ? msg.getCisNumber()
                : "unknown");

    // Final blob path: <source>/<batchId>/<consumer>/<folder>/<accountOrCis>_<fileName>
    return sourceSystem + "/" +
            msg.getBatchId() + "/" +
            consumerRef + "/" +
            folderName + "/" +
            accountOrCis + "_" + fileName;
}
