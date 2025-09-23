private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

    Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();

    try (Stream<Path> stream = Files.walk(jobDir)) {
        stream.filter(Files::isRegularFile).forEach(file -> {
            if (!Files.exists(file)) return;

            String fileName = file.getFileName().toString();
            String lowerFileName = fileName.toLowerCase();
            String parentFolder = file.getParent().getFileName().toString().toLowerCase();

            boolean allowed = false;
            if (parentFolder.contains(FOLDER_ARCHIVE) || parentFolder.contains(FOLDER_MOBSTAT) || parentFolder.contains(FOLDER_PRINT)) {
                allowed = lowerFileName.endsWith(".pdf") || lowerFileName.endsWith(".ps");
            } else if (parentFolder.contains(FOLDER_EMAIL)) {
                allowed = lowerFileName.endsWith(".pdf") || lowerFileName.endsWith(".html") || lowerFileName.endsWith(".txt") || lowerFileName.endsWith(".text");
            }
            if (!allowed) return;

            try {
                String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), parentFolder, msg));

                for (SummaryProcessedFile customer : customerList) {
                    if (customer == null || customer.getAccountNumber() == null) continue;
                    String account = customer.getAccountNumber();
                    if (!fileName.contains(account)) continue;

                    if (parentFolder.contains(FOLDER_ARCHIVE)) {
                        accountToArchiveFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains(FOLDER_EMAIL)) {
                        accountToEmailFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains(FOLDER_MOBSTAT)) {
                        accountToMobstatFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains(FOLDER_PRINT)) {
                        accountToPrintFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    }
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to upload file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
            }
        });
    }

    List<PrintFile> printFiles = new ArrayList<>();

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        Map<String, String> archivesForAccount = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> emailsForAccount = accountToEmailFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> mobstatsForAccount = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> printsForAccount = accountToPrintFiles.getOrDefault(account, Collections.emptyMap());

        // Skip if nothing exists
        if (archivesForAccount.isEmpty() && emailsForAccount.isEmpty() &&
                mobstatsForAccount.isEmpty() && printsForAccount.isEmpty()) continue;

        SummaryProcessedFile entry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, entry);

        // Archive
        entry.setArchiveBlobUrl(archivesForAccount.isEmpty() ? null : archivesForAccount.values().iterator().next());
        entry.setArchiveStatus(entry.getArchiveBlobUrl() != null ? "SUCCESS" : null);

        // Combine all email URLs into entry
        for (Map.Entry<String, String> e : emailsForAccount.entrySet()) {
            String fname = e.getKey();
            String url = e.getValue();
            if (fname.toLowerCase().endsWith(".pdf")) entry.setPdfEmailFileUrl(url);
            else if (fname.toLowerCase().endsWith(".html")) entry.setHtmlEmailFileUrl(url);
            else if (fname.toLowerCase().endsWith(".txt") || fname.toLowerCase().endsWith(".text")) entry.setTextEmailFileUrl(url);
        }
        if (!emailsForAccount.isEmpty()) entry.setEmailStatus("SUCCESS");

        // Mobstat
        if (!mobstatsForAccount.isEmpty()) {
            entry.setPdfMobstatFileUrl(mobstatsForAccount.values().iterator().next());
            entry.setPdfMobstatStatus("SUCCESS");
        }

        entry.setOverallStatus("SUCCESS");
        finalList.add(entry);

        // Print files (.ps only)
        for (Map.Entry<String, String> p : printsForAccount.entrySet()) {
            String fname = p.getKey();
            String url = p.getValue();
            if (!fname.toLowerCase().endsWith(".ps")) continue;

            SummaryProcessedFile printEntry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, printEntry);
            printEntry.setPrintFileUrl(url);
            printEntry.setPrintStatus("SUCCESS");
            printEntry.setOverallStatus("SUCCESS");
            finalList.add(printEntry);

            PrintFile pf = new PrintFile();
            pf.setPrintFileURL(url);
            pf.setPrintStatus("SUCCESS");
            printFiles.add(pf);
        }
    }

    msg.setPrintFiles(printFiles);
    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

public static SummaryPayload buildPayload(
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        String fileName,
        String batchId,
        String timestamp,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles
) {
    if (kafkaMessage == null) return new SummaryPayload();
    if (processedList == null) processedList = Collections.emptyList();
    if (errorMap == null) errorMap = Collections.emptyMap();
    if (printFiles == null) printFiles = Collections.emptyList();

    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);

    payload.setHeader(buildHeader(kafkaMessage, timestamp));

    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap, printFiles);
    payload.setProcessedFileList(processedFileEntries);

    payload.setPayload(buildPayloadInfo(kafkaMessage, processedFileEntries));

    Metadata metadata = buildMetadata(processedFileEntries, batchId, fileName, kafkaMessage);
    payload.setMetadata(metadata);

    List<PrintFile> printFileList = processPrintFiles(printFiles, errorMap, batchId, fileName);
    payload.setPrintFiles(printFileList);

    return payload;
}

// ------------------- HEADER -------------------
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

    return metadata;
}

// ------------------- PRINT FILES -------------------
private static List<PrintFile> processPrintFiles(List<PrintFile> printFiles, Map<String, Map<String, String>> errorMap, String batchId, String fileName) {
    List<PrintFile> result = new ArrayList<>();
    for (PrintFile pf : printFiles) {
        if (pf == null) continue;
        String psUrl = pf.getPrintFileURL();
        if (psUrl == null || !psUrl.toLowerCase().endsWith(".ps")) continue;
        pf.setPrintFileURL(URLDecoder.decode(psUrl, StandardCharsets.UTF_8));
        pf.setPrintStatus("SUCCESS");
        result.add(pf);
    }
    return result;
}

// ------------------- PROCESSED FILE ENTRIES -------------------
private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles,
                                                                  Map<String, Map<String, String>> errorMap,
                                                                  List<PrintFile> ignoredPrintFiles) {
    processedFiles = validateProcessedFiles(processedFiles, ignoredPrintFiles);
    if (processedFiles.isEmpty()) return Collections.emptyList();
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
    }
    return new ArrayList<>(groupedMap.values());
}

// ------------------- HELPER METHODS -------------------
private static List<SummaryProcessedFile> validateProcessedFiles(List<SummaryProcessedFile> processedFiles, List<PrintFile> ignoredPrintFiles) {
    if (processedFiles == null) return Collections.emptyList();
    return processedFiles;
}

private static boolean isNonEmpty(String s) {
    return s != null && !s.isBlank();
}

private static String determineOverallStatus(ProcessedFileEntry entry, String account, Map<String, Map<String, String>> errorMap) {
    boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus()) || "SUCCESS".equals(entry.getHtmlEmailBlobUrl()) || "SUCCESS".equals(entry.getTextEmailBlobUrl());
    boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
    boolean printSuccess  = "SUCCESS".equals(entry.getPrintStatus());
    boolean archiveSuccess= "SUCCESS".equals(entry.getArchiveStatus());

    String overallStatus;
    if ((emailSuccess && archiveSuccess) || (mobstatSuccess && archiveSuccess && !emailSuccess && !printSuccess)
            || (printSuccess && archiveSuccess && !emailSuccess && !mobstatSuccess)) {
        overallStatus = "SUCCESS";
    } else if (archiveSuccess) overallStatus = "PARTIAL";
    else overallStatus = "FAILED";

    if (errorMap.containsKey(account) && !"FAILED".equals(overallStatus)) overallStatus = "PARTIAL";
    return overallStatus;
}


