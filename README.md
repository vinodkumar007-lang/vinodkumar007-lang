public static SummaryPayload buildPayload(
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        String summaryBlobUrl,
        String fileName,
        String batchId,
        String timestamp,
        Map<String, Map<String, String>> errorMap
) {
    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);
    payload.setSummaryFileURL(summaryBlobUrl);

    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setTimestamp(timestamp);
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setProduct(kafkaMessage.getSourceSystem());
    header.setJobName(kafkaMessage.getSourceSystem());
    payload.setHeader(header);

    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap);
    payload.setProcessedFileList(processedFileEntries);

    int totalFileUrls = (int) processedFileEntries.stream()
            .flatMap(entry -> Stream.of(
                    new AbstractMap.SimpleEntry<>(entry.getEmailBlobUrl(), entry.getEmailStatus()),
                    new AbstractMap.SimpleEntry<>(entry.getPrintBlobUrl(), entry.getPrintStatus()),
                    new AbstractMap.SimpleEntry<>(entry.getMobstatBlobUrl(), entry.getMobstatStatus()),
                    new AbstractMap.SimpleEntry<>(entry.getArchiveBlobUrl(), entry.getArchiveStatus())
            ))
            .filter(e -> e.getKey() != null && !e.getKey().trim().isEmpty()
                    && "SUCCESS".equalsIgnoreCase(e.getValue()))
            .count();

    Payload payloadInfo = new Payload();
    payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
    payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
    payloadInfo.setEventID(kafkaMessage.getEventID());
    payloadInfo.setEventType(kafkaMessage.getEventType());
    payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
    payloadInfo.setFileCount(totalFileUrls);
    payload.setPayload(payloadInfo);

    Metadata metadata = new Metadata();
    metadata.setTotalCustomersProcessed((int) processedFileEntries.stream()
            .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
            .distinct()
            .count());

    long total = processedFileEntries.size();
    long success = processedFileEntries.stream()
            .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverallStatus()))
            .count();
    long failed = processedFileEntries.stream()
            .filter(entry -> "FAILED".equalsIgnoreCase(entry.getOverallStatus()))
            .count();

    String overallStatus;
    if (success == total) {
        overallStatus = "SUCCESS";
    } else if (failed == total) {
        overallStatus = "FAILED";
    } else {
        overallStatus = "PARTIAL";
    }

    metadata.setProcessingStatus(overallStatus);
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
    payload.setMetadata(metadata);

    return payload;
}

private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, String> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();

        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputType = file.getOutputType();
        String blobUrl = file.getBlobUrl();
        String status = file.getStatus();

        // Set blob URL and status based on outputType
        switch (outputType) {
            case "EMAIL":
                entry.setEmailBlobUrl(blobUrl);
                entry.setEmailStatus(status);
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(blobUrl);
                entry.setArchiveStatus(status);
                break;
            case "PRINT":
                entry.setPrintBlobUrl(blobUrl);
                entry.setPrintStatus(status);
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(blobUrl);
                entry.setMobstatStatus(status);
                break;
        }

        grouped.put(key, entry);
    }

    // Post-processing for each entry to apply edge-case rules
    for (ProcessedFileEntry entry : grouped.values()) {
        String overallStatus = "FAILED"; // default
        boolean archiveSuccess = "SUCCESS".equalsIgnoreCase(entry.getArchiveStatus());
        boolean emailSuccess = "SUCCESS".equalsIgnoreCase(entry.getEmailStatus());
        boolean emailExists = entry.getEmailBlobUrl() != null && !entry.getEmailBlobUrl().isEmpty();
        boolean hasError = errorMap.containsKey(entry.getAccountNumber());

        // Case 1: Both email & archive are success
        if (archiveSuccess && emailSuccess) {
            overallStatus = "SUCCESS";
        }
        // Case 2: Email not found but account in errorMap => FAILED
        else if (archiveSuccess && (entry.getEmailBlobUrl() == null || entry.getEmailBlobUrl().isEmpty())) {
            if (hasError) {
                entry.setEmailStatus("FAILED");
                overallStatus = "FAILED";
            } else {
                // File missing but not in error list — consider as partial
                entry.setEmailStatus("NOT_FOUND");
                overallStatus = "PARTIAL";
            }
        }
        // Case 3: One success, one fail => PARTIAL
        else if ((emailSuccess && !archiveSuccess) || (!emailSuccess && archiveSuccess)) {
            overallStatus = "PARTIAL";
        }

        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}


