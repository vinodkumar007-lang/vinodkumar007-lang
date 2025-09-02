Comment 1: "Can this function be split into manageable chunks?"

✅ Done. I refactored buildPayload into smaller helper methods: buildHeader, buildPayloadInfo, buildMetadata, and processPrintFiles. Each helper handles a specific part of the payload, improving readability and maintainability without changing existing logic.

Comment 2: "Is this not duplication for the above for loop, can they be merged?"

✅ Addressed. The two for loops over printFiles were merged into a single loop that both assigns the status and decodes the URL. This preserves the original behavior while removing duplication.

public static SummaryPayload buildPayload(
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        String fileName,
        String batchId,
        String timestamp,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles
) {
    // --- Validation ---
    if (kafkaMessage == null) {
        logger.error("[buildPayload] kafkaMessage is null. Returning empty payload. batchId={}, fileName={}", batchId, fileName);
        return new SummaryPayload();
    }
    if (processedList == null) {
        logger.warn("[buildPayload] processedList is null. Using empty list. batchId={}, fileName={}", batchId, fileName);
        processedList = Collections.emptyList();
    }
    if (errorMap == null) {
        logger.warn("[buildPayload] errorMap is null. Defaulting to empty map. batchId={}, fileName={}", batchId, fileName);
        errorMap = Collections.emptyMap();
    }
    if (printFiles == null) {
        logger.warn("[buildPayload] printFiles is null. Using empty list. batchId={}, fileName={}", batchId, fileName);
        printFiles = Collections.emptyList();
    }

    logger.info("[GT] Start building payload. batchId={}, fileName={}, processedListSize={}, printFilesSize={}",
            batchId, fileName, processedList.size(), printFiles.size());

    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);

    // --- Header ---
    payload.setHeader(buildHeader(kafkaMessage, timestamp));

    // --- Processed files ---
    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap, printFiles);
    payload.setProcessedFileList(processedFileEntries);

    // --- Payload Info ---
    payload.setPayload(buildPayloadInfo(kafkaMessage, processedFileEntries));

    // --- Metadata ---
    Metadata metadata = buildMetadata(processedFileEntries, batchId, fileName);
    payload.setMetadata(metadata);

    // --- Print Files ---
    List<PrintFile> printFileList = processPrintFiles(printFiles, errorMap, batchId, fileName);
    payload.setPrintFiles(printFileList);

    logger.info("[GT] Completed building payload. batchId={}, fileName={}, processedEntries={}, printFiles={}",
            batchId, fileName, processedFileEntries.size(), printFileList.size());

    return payload;
}

// ---------- Helper Methods ------------

private static Header buildHeader(KafkaMessage kafkaMessage, String timestamp) {
    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setTimestamp(timestamp);
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setProduct(kafkaMessage.getSourceSystem());
    header.setJobName(kafkaMessage.getSourceSystem());
    return header;
}

private static Payload buildPayloadInfo(KafkaMessage kafkaMessage, List<ProcessedFileEntry> processedFileEntries) {
    int totalUniqueFiles = (int) processedFileEntries.stream()
            .flatMap(entry -> Stream.of(
                    entry.getEmailBlobUrl(),
                    entry.getPrintBlobUrl(),
                    entry.getMobstatBlobUrl(),
                    entry.getArchiveBlobUrl()
            ))
            .filter(Objects::nonNull)
            .distinct()
            .count();

    Payload payloadInfo = new Payload();
    payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
    payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
    payloadInfo.setEventID(kafkaMessage.getEventID());
    payloadInfo.setEventType(kafkaMessage.getEventType());
    payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
    payloadInfo.setFileCount(totalUniqueFiles);
    return payloadInfo;
}

private static Metadata buildMetadata(List<ProcessedFileEntry> processedFileEntries, String batchId, String fileName) {
    Metadata metadata = new Metadata();

    long totalArchiveEntries = processedFileEntries.stream()
            .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl())).distinct()
            .count();
    metadata.setTotalCustomersProcessed((int) totalArchiveEntries);

    Set<String> statuses = processedFileEntries.stream()
            .map(ProcessedFileEntry::getOverallStatus)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

    String overallStatus;
    if (statuses.size() == 1) {
        overallStatus = statuses.iterator().next();
    } else if (statuses.contains("SUCCESS") && statuses.contains("FAILED")) {
        overallStatus = "PARTIAL";
    } else if (statuses.contains("PARTIAL") || statuses.size() > 1) {
        overallStatus = "PARTIAL";
    } else {
        overallStatus = "FAILED";
    }

    metadata.setProcessingStatus(overallStatus);
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription(overallStatus.toLowerCase());

    logger.info("[GT] Metadata built. batchId={}, fileName={}, totalCustomers={}, overallStatus={}",
            batchId, fileName, metadata.getTotalCustomersProcessed(), overallStatus);

    return metadata;
}

private static List<PrintFile> processPrintFiles(List<PrintFile> printFiles, Map<String, Map<String, String>> errorMap, String batchId, String fileName) {
    List<PrintFile> result = new ArrayList<>();

    for (PrintFile pf : printFiles) {
        if (pf == null) {
            logger.debug("[buildPayload] Skipping null PrintFile. batchId={}, fileName={}", batchId, fileName);
            continue;
        }

        String psUrl = pf.getPrintFileURL();
        String status = "";
        if (psUrl != null && psUrl.endsWith(".ps")) {
            status = "SUCCESS";
        } else if (psUrl != null && errorMap.containsKey(psUrl)) {
            status = "FAILED";
        }

        String decodedUrl = psUrl != null ? URLDecoder.decode(psUrl, StandardCharsets.UTF_8) : null;

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL(decodedUrl);
        printFile.setPrintStatus(status);

        result.add(printFile);

        logger.debug("[GT] PrintFile processed. batchId={}, fileName={}, psUrl={}, status={}",
                batchId, fileName, decodedUrl, status);
    }

    return result;
}
