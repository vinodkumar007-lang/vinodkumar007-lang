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

    // HEADER
    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setTimestamp(timestamp);
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setProduct(kafkaMessage.getSourceSystem());
    header.setJobName(kafkaMessage.getSourceSystem());
    payload.setHeader(header);

    // METADATA
    Metadata metadata = new Metadata();
    metadata.setTotalCustomersProcessed((int) processedList.stream()
            .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
            .distinct()
            .count());
    metadata.setProcessingStatus("Completed");
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription("Success");
    payload.setMetadata(metadata);

    // PAYLOAD BLOCK
    Payload payloadInfo = new Payload();
    payloadInfo.setUniqueECPBatchRef(null);
    payloadInfo.setRunPriority(null);
    payloadInfo.setEventID(null);
    payloadInfo.setEventType(null);
    payloadInfo.setRestartKey(null);

    // ✅ Final Processed Entries
    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
    payload.setProcessedFileList(processedFileEntries);

    // ✅ fileCount = entries added to summary.json
    payloadInfo.setFileCount(processedFileEntries.size());
    payload.setPayload(payloadInfo);

    return payload;
}
