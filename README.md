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
    payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
    payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
    payloadInfo.setEventID(kafkaMessage.getEventID());
    payloadInfo.setEventType(kafkaMessage.getEventType());
    payloadInfo.setRestartKey(kafkaMessage.getRestartKey());

    // ✅ Final Processed Entries
    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
    payload.setProcessedFileList(processedFileEntries);

    // ✅ Dynamically count total non-null URLs (files actually added to summary)
    int totalFileUrls = processedFileEntries.stream()
            .mapToInt(entry -> {
                int count = 0;
                if (entry.getPdfEmailFileUrl() != null && !entry.getPdfEmailFileUrl().isBlank()) count++;
                if (entry.getPdfArchiveFileUrl() != null && !entry.getPdfArchiveFileUrl().isBlank()) count++;
                if (entry.getPdfMobstatFileUrl() != null && !entry.getPdfMobstatFileUrl().isBlank()) count++;
                if (entry.getPrintFileUrl() != null && !entry.getPrintFileUrl().isBlank()) count++;
                return count;
            })
            .sum();

    payloadInfo.setFileCount(totalFileUrls);
    payload.setPayload(payloadInfo);

    return payload;
}

