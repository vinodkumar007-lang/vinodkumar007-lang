private static final Logger log = LoggerFactory.getLogger(YourClassName.class);

// ------------------- BUILD PAYLOAD -------------------
public static SummaryPayload buildPayload(
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        String fileName,
        String batchId,
        String timestamp,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles
) {
    log.info("[BUILD PAYLOAD] Start building SummaryPayload for batchId={}, fileName={}, timestamp={}", batchId, fileName, timestamp);

    if (kafkaMessage == null) {
        log.warn("[BUILD PAYLOAD] kafkaMessage is null, returning empty payload");
        return new SummaryPayload();
    }
    if (processedList == null) processedList = Collections.emptyList();
    if (errorMap == null) errorMap = Collections.emptyMap();
    if (printFiles == null) printFiles = Collections.emptyList();

    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);

    log.debug("[HEADER] Building header from KafkaMessage");
    payload.setHeader(buildHeader(kafkaMessage, timestamp));

    log.debug("[PROCESSED FILE ENTRIES] Building processed file entries");
    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap, printFiles);
    payload.setProcessedFileList(processedFileEntries);

    log.debug("[PAYLOAD INFO] Building payload info");
    payload.setPayload(buildPayloadInfo(kafkaMessage, processedFileEntries));

    log.debug("[METADATA] Building metadata");
    Metadata metadata = buildMetadata(processedFileEntries, batchId, fileName, kafkaMessage);
    payload.setMetadata(metadata);

    log.debug("[PRINT FILES] Processing print files");
    List<PrintFile> printFileList = processPrintFiles(printFiles, errorMap, batchId, fileName);
    payload.setPrintFiles(printFileList);

    log.info("[BUILD PAYLOAD] Completed for batchId={}, overallStatus={}", batchId, metadata.getProcessingStatus());
    return payload;
}

// ------------------- HEADER -------------------
private static Header buildHeader(KafkaMessage kafkaMessage, String timestamp) {
    log.debug("[HEADER] tenantCode={}, channelID={}, audienceID={}", 
              kafkaMessage.getTenantCode(), kafkaMessage.getChannelID(), kafkaMessage.getAudienceID());

    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setTimestamp(timestamp);
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setProduct(kafkaMessage.getSourceSystem());
    header.setJobName(kafkaMessage.getSourceSystem());

    log.debug("[HEADER] Completed with tenantCode={}, sourceSystem={}", header.getTenantCode(), header.getSourceSystem());
    return header;
}

// ------------------- PAYLOAD INFO -------------------
private static Payload buildPayloadInfo(KafkaMessage kafkaMessage, List<ProcessedFileEntry> processedFileEntries) {
    int totalUniqueFiles = (int) processedFileEntries.stream()
            .flatMap(entry -> Stream.of(
                    entry.getEmailBlobUrl(),
                    entry.getHtmlEmailBlobUrl(),
                    entry.getTextEmailBlobUrl(),
                    entry.getPrintBlobUrl(),
                    entry.getMobstatBlobUrl(),
                    entry.getArchiveBlobUrl()
            ))
            .filter(Objects::nonNull)
            .distinct()
            .count();

    log.debug("[PAYLOAD INFO] totalUniqueFiles={}, eventID={}, eventType={}", totalUniqueFiles, kafkaMessage.getEventID(), kafkaMessage.getEventType());

    Payload payloadInfo = new Payload();
    payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
    payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
    payloadInfo.setEventID(kafkaMessage.getEventID());
    payloadInfo.setEventType(kafkaMessage.getEventType());
    payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
    payloadInfo.setFileCount(totalUniqueFiles);

    return payloadInfo;
}

// ------------------- METADATA -------------------
private static Metadata buildMetadata(List<ProcessedFileEntry> processedFileEntries, String batchId, String fileName, KafkaMessage kafkaMessage) {
    Metadata metadata = new Metadata();

    Set<String> customersWithFiles = processedFileEntries.stream()
            .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl()) ||
                    isNonEmpty(pf.getEmailBlobUrl()) ||
                    isNonEmpty(pf.getHtmlEmailBlobUrl()) ||
                    isNonEmpty(pf.getTextEmailBlobUrl()) ||
                    isNonEmpty(pf.getPrintBlobUrl()) ||
                    isNonEmpty(pf.getMobstatBlobUrl()))
            .map(ProcessedFileEntry::getAccountNumber)
            .collect(Collectors.toSet());

    metadata.setTotalCustomersProcessed(customersWithFiles.size());

    Set<String> statuses = processedFileEntries.stream()
            .map(ProcessedFileEntry::getOverallStatus)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

    String overallStatus;
    if (statuses.size() == 1) overallStatus = statuses.iterator().next();
    else if (statuses.contains("SUCCESS") && statuses.contains("FAILED")) overallStatus = "PARTIAL";
    else if (statuses.contains("PARTIAL") || statuses.size() > 1) overallStatus = "PARTIAL";
    else overallStatus = "FAILED";

    int customerCount = kafkaMessage.getBatchFiles().stream()
            .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
            .mapToInt(BatchFile::getCustomerCount)
            .sum();
    metadata.setCustomerCount(customerCount);
    metadata.setProcessingStatus(overallStatus);
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription(overallStatus.toLowerCase());

    log.info("[METADATA] batchId={}, fileName={}, totalCustomers={}, customerCount={}, status={}",
             batchId, fileName, customersWithFiles.size(), customerCount, overallStatus);

    return metadata;
}

// ------------------- PRINT FILES -------------------
private static List<PrintFile> processPrintFiles(List<PrintFile> printFiles, Map<String, Map<String, String>> errorMap, String batchId, String fileName) {
    List<PrintFile> result = new ArrayList<>();
    for (PrintFile pf : printFiles) {
        if (pf == null) continue;
        String psUrl = pf.getPrintFileURL();
        if (psUrl == null || !psUrl.toLowerCase().endsWith(".ps")) {
            log.debug("[PRINT FILES] Skipping non-ps file: {}", psUrl);
            continue;
        }
        pf.setPrintFileURL(URLDecoder.decode(psUrl, StandardCharsets.UTF_8));
        pf.setPrintStatus("SUCCESS");
        log.debug("[PRINT FILES] Added print file: {}", pf.getPrintFileURL());
        result.add(pf);
    }
    log.info("[PRINT FILES] Total PS files processed: {}", result.size());
    return result;
}

// ------------------- PROCESSED FILE ENTRIES -------------------
private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles,
                                                                  Map<String, Map<String, String>> errorMap,
                                                                  List<PrintFile> ignoredPrintFiles) {
    processedFiles = validateProcessedFiles(processedFiles, ignoredPrintFiles);
    if (processedFiles.isEmpty()) {
        log.warn("[PROCESSED FILE ENTRIES] No processed files found");
        return Collections.emptyList();
    }
    if (errorMap == null) errorMap = Collections.emptyMap();

    Map<String, ProcessedFileEntry> groupedMap = new HashMap<>();
    for (SummaryProcessedFile file : processedFiles) {
        if (file == null) continue;
        String account = file.getAccountNumber();
        if (account == null) continue;

        ProcessedFileEntry entry = groupedMap.computeIfAbsent(account, k -> new ProcessedFileEntry());
        if (entry.getCustomerId() == null) entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(account);

        if (isNonEmpty(file.getPdfEmailFileUrl())) entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        if (isNonEmpty(file.getHtmlEmailFileUrl())) entry.setHtmlEmailBlobUrl(file.getHtmlEmailFileUrl());
        if (isNonEmpty(file.getTextEmailFileUrl())) entry.setTextEmailBlobUrl(file.getTextEmailFileUrl());
        if (isNonEmpty(file.getPdfMobstatFileUrl())) entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        if (isNonEmpty(file.getPrintFileUrl())) entry.setPrintBlobUrl(file.getPrintFileUrl());
        if (isNonEmpty(file.getArchiveBlobUrl())) entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

        entry.setEmailStatus(isNonEmpty(entry.getEmailBlobUrl()) || isNonEmpty(entry.getHtmlEmailBlobUrl()) || isNonEmpty(entry.getTextEmailBlobUrl()) ? "SUCCESS" : "");
        entry.setMobstatStatus(isNonEmpty(entry.getMobstatBlobUrl()) ? "SUCCESS" : "");
        entry.setPrintStatus(isNonEmpty(entry.getPrintBlobUrl()) ? "SUCCESS" : "");
        entry.setArchiveStatus(isNonEmpty(entry.getArchiveBlobUrl()) ? "SUCCESS" : "");

        entry.setOverallStatus(determineOverallStatus(entry, account, errorMap));
        log.debug("[PROCESSED FILE ENTRY] account={}, statuses=[email={}, mobstat={}, print={}, archive={}], overall={}",
                  account, entry.getEmailStatus(), entry.getMobstatStatus(), entry.getPrintStatus(), entry.getArchiveStatus(), entry.getOverallStatus());
    }

    log.info("[PROCESSED FILE ENTRIES] Built {} entries", groupedMap.size());
    return new ArrayList<>(groupedMap.values());
}
