private String sanitize(String value, String fallback) {
    if (value == null || value.trim().isEmpty()) return fallback;
    return value.replaceAll("[^a-zA-Z0-9-_]", "_"); // allow alphanumeric, hyphen, underscore
}

private String buildBlobPath(String fileName, String folderName, KafkaMessage msg) {
    String sourceSystem = sanitize(msg.getSourceSystem(), "UNKNOWN_SOURCE");
    String consumerRef = sanitize(msg.getUniqueConsumerRef(), "UNKNOWN_CONSUMER");

    return sourceSystem + "/" +
           consumerRef + "/" +
           folderName + "/" +
           fileName;
}
