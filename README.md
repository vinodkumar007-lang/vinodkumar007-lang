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
    metadata.setTotalFilesProcessed(processedList.size());
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
    payloadInfo.setFileCount(processedList.size());
    payload.setPayload(payloadInfo);

    // ✅ PROCESSED FILE LIST (grouped by customer + account)
    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
    payload.setProcessedFileList(processedFileEntries);

    // ✅ TRIGGER FILE
    payload.setMobstatTriggerFile(buildMobstatTrigger(processedList));

    return payload;
}

private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    Map<String, ProcessedFileEntry> entryMap = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedList) {
        if (file.getCustomerId() == null || file.getAccountNumber() == null) continue;

        String key = file.getCustomerId() + "::" + file.getAccountNumber();
        ProcessedFileEntry entry = entryMap.computeIfAbsent(key, k -> {
            ProcessedFileEntry e = new ProcessedFileEntry();
            e.setCustomerId(file.getCustomerId());
            e.setAccountNumber(file.getAccountNumber());
            return e;
        });

        String url = file.getBlobFileURL();
        if (url == null) continue;

        if (url.contains("/email/")) {
            entry.setEmailFileURL(url);
            entry.setEmailStatus("Success");
        } else if (url.contains("/archive/")) {
            entry.setArchiveFileURL(url);
            entry.setArchiveStatus("Success");
        } else if (url.contains("/mobstat/")) {
            entry.setMobstatFileURL(url);
            entry.setMobstatStatus("Success");
        } else if (url.contains("/print/")) {
            entry.setPrintFileURL(url);
            entry.setPrintStatus("Success");
        }
    }

    return new ArrayList<>(entryMap.values());
}
