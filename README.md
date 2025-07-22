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
        Map<String, Map<String, String>> errorReportMap
) {
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        switch (file.getOutputType()) {
            case "EMAIL":
                entry.setEmailBlobUrl(file.getBlobUrl());
                entry.setEmailStatus(file.getStatus());
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(file.getBlobUrl());
                entry.setArchiveStatus(file.getStatus());
                break;
            case "PRINT":
                entry.setPrintBlobUrl(file.getBlobUrl());
                entry.setPrintStatus(file.getStatus());
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(file.getBlobUrl());
                entry.setMobstatStatus(file.getStatus());
                break;
        }

        grouped.put(key, entry);
    }

    // Finalize statuses
    for (ProcessedFileEntry entry : grouped.values()) {
        boolean archiveSuccess = "SUCCESS".equalsIgnoreCase(entry.getArchiveStatus());

        boolean emailFailed = isFailedOrMissing(entry.getEmailStatus(), entry.getCustomerId(), entry.getAccountNumber(), "EMAIL", errorReportMap);
        boolean printFailed = isFailedOrMissing(entry.getPrintStatus(), entry.getCustomerId(), entry.getAccountNumber(), "PRINT", errorReportMap);
        boolean mobstatFailed = isFailedOrMissing(entry.getMobstatStatus(), entry.getCustomerId(), entry.getAccountNumber(), "MOBSTAT", errorReportMap);

        boolean anyFailed = emailFailed || printFailed || mobstatFailed;
        boolean allMissing = isAllMissing(entry);

        if (!archiveSuccess) {
            entry.setOverallStatus("FAILED");
        } else if (anyFailed) {
            entry.setOverallStatus("FAILED"); // You can switch this to "PARTIAL" if your rule permits
        } else if (archiveSuccess) {
            entry.setOverallStatus("SUCCESS");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(grouped.values());
}

private static boolean isFailedOrMissing(
        String status,
        String customerId,
        String accountNumber,
        String type,
        Map<String, Map<String, String>> errorMap
) {
    if ("FAILED".equalsIgnoreCase(status)) return true;

    boolean isMissing = (status == null || status.isEmpty());

    if (isMissing) {
        Map<String, String> customerErrors = errorMap.getOrDefault(customerId, Collections.emptyMap());
        return customerErrors.containsKey(accountNumber);
    }

    return false;
}

private static boolean isAllMissing(ProcessedFileEntry entry) {
    return isNullOrEmpty(entry.getEmailStatus()) &&
           isNullOrEmpty(entry.getPrintStatus()) &&
           isNullOrEmpty(entry.getMobstatStatus());
}

private static boolean isNullOrEmpty(String s) {
    return s == null || s.trim().isEmpty();
}


