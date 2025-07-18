public static SummaryPayload buildPayload(
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        String summaryBlobUrl,
        String fileName,
        String batchId,
        String timestamp
) {
    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);
    payload.setSummaryFileURL(summaryBlobUrl);

    // Header section from Kafka
    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setConsumerReference(kafkaMessage.getConsumerReference());
    header.setProcessReference(kafkaMessage.getProcessReference());
    payload.setHeader(header);

    // Metadata grouping processed files by customerId
    Metadata metadata = new Metadata();
    metadata.setCustomerSummaries(groupProcessedFilesByCustomerId(processedList));
    payload.setMetadata(metadata);

    // Flat processed list also set
    payload.setProcessedList(processedList);

    return payload;
}

private static List<CustomerSummary> groupProcessedFilesByCustomerId(List<SummaryProcessedFile> processedList) {
    Map<String, CustomerSummary> customerMap = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedList) {
        String customerId = file.getCustomerId();
        if (customerId == null || customerId.isBlank()) continue;

        CustomerSummary summary = customerMap.computeIfAbsent(customerId, id -> {
            CustomerSummary cs = new CustomerSummary();
            cs.setCustomerId(id);
            cs.setFiles(new ArrayList<>());
            return cs;
        });

        summary.getFiles().add(file);
    }

    return new ArrayList<>(customerMap.values());
}

public static String writeSummaryJsonToFile(SummaryPayload payload) {
    if (payload == null) {
        logger.error("SummaryPayload is null. Cannot write summary.json.");
        throw new IllegalArgumentException("SummaryPayload cannot be null");
    }

    try {
        String batchId = Optional.ofNullable(payload.getBatchID()).orElse("unknown");
        String fileName = "summary_" + batchId + ".json";

        Path tempDir = Files.createTempDirectory("summaryFiles");
        Path summaryFilePath = tempDir.resolve(fileName);

        File summaryFile = summaryFilePath.toFile();
        if (summaryFile.exists()) {
            Files.delete(summaryFilePath);
            logger.warn("Existing summary file deleted: {}", summaryFilePath);
        }

        objectMapper.writeValue(summaryFile, payload);
        logger.info("✅ Summary JSON written at: {}", summaryFilePath);

        return summaryFilePath.toAbsolutePath().toString();

    } catch (Exception e) {
        logger.error("❌ Failed to write summary.json", e);
        throw new RuntimeException("Failed to write summary JSON", e);
    }
}
