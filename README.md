// -------------------- buildDetailedProcessedFiles --------------------
private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

    // Maps to hold uploaded files per account
    Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();
    List<PrintFile> printFiles = new ArrayList<>();

    // -------- Upload & categorize files --------
    try (Stream<Path> stream = Files.walk(jobDir)) {
        stream.filter(Files::isRegularFile).forEach(file -> {
            if (!Files.exists(file)) return;

            String fileName = file.getFileName().toString().toLowerCase();
            String parentFolder = file.getParent().getFileName().toString().toLowerCase();

            try {
                String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), parentFolder, msg));

                for (SummaryProcessedFile customer : customerList) {
                    if (customer == null || customer.getAccountNumber() == null) continue;
                    String account = customer.getAccountNumber();
                    if (!fileName.contains(account)) continue;

                    if (parentFolder.contains("archive") && (fileName.endsWith(".pdf") || fileName.endsWith(".ps"))) {
                        accountToArchiveFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains("email") && (fileName.endsWith(".pdf") || fileName.endsWith(".html") || fileName.endsWith(".txt") || fileName.endsWith(".text"))) {
                        accountToEmailFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains("mobstat") && fileName.endsWith(".pdf")) {
                        accountToMobstatFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains("print") && fileName.endsWith(".ps")) {
                        accountToPrintFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);

                        PrintFile pf = new PrintFile();
                        pf.setPrintFileURL(url);
                        pf.setPrintStatus("SUCCESS");
                        printFiles.add(pf);
                    }
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to upload file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
            }
        });
    }

    // -------- Build final list for summary.json --------
    Set<String> uniqueKeys = new HashSet<>();
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        Map<String, String> archives = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> emails = accountToEmailFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> mobstats = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());

        List<String> archiveFiles = archives.isEmpty() ? Arrays.asList((String) null) : new ArrayList<>(archives.keySet());
        List<String> emailFiles = emails.isEmpty() ? Arrays.asList((String) null) : new ArrayList<>(emails.values());
        List<String> mobstatFiles = mobstats.isEmpty() ? Arrays.asList((String) null) : new ArrayList<>(mobstats.values());

        for (String archiveFile : archiveFiles) {
            for (String emailUrl : emailFiles) {
                for (String mobstatUrl : mobstatFiles) {
                    String key = customer.getCustomerId() + "|" + account + "|" +
                            (archiveFile != null ? archiveFile : "noArchive") + "|" +
                            (emailUrl != null ? emailUrl : "noEmail") + "|" +
                            (mobstatUrl != null ? mobstatUrl : "noMobstat");
                    if (uniqueKeys.contains(key)) continue;
                    uniqueKeys.add(key);

                    SummaryProcessedFile entry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, entry);
                    entry.setArchiveBlobUrl(archiveFile != null ? archives.get(archiveFile) : null);
                    entry.setPdfEmailFileUrl(emailUrl);
                    entry.setPdfMobstatFileUrl(mobstatUrl);
                    entry.setOverallStatus("SUCCESS");
                    finalList.add(entry);
                }
            }
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

// -------------------- buildPayload --------------------
public static SummaryPayload buildPayload(
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        String fileName,
        String batchId,
        String timestamp,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles
) {
    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);

    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setTimestamp(timestamp);
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setProduct(kafkaMessage.getSourceSystem());
    header.setJobName(kafkaMessage.getSourceSystem());
    payload.setHeader(header);

    // Build ProcessedFileEntry list
    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap);
    payload.setProcessedFileList(processedFileEntries);

    // File count (PDF + HTML + TXT + Mobstat + Archive)
    int totalUniqueFiles = (int) processedList.stream()
            .flatMap(entry -> Stream.of(
                    entry.getPdfEmailFileUrl(),
                    entry.getEmailBlobUrlHtml(),
                    entry.getEmailBlobUrlText(),
                    entry.getPdfMobstatFileUrl(),
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
    payload.setPayload(payloadInfo);

    // Metadata
    Metadata metadata = new Metadata();
    Set<String> uniqueCustomers = processedList.stream()
            .filter(p -> isNonEmpty(p.getArchiveBlobUrl()) ||
                         isNonEmpty(p.getPdfEmailFileUrl()) ||
                         isNonEmpty(p.getEmailBlobUrlHtml()) ||
                         isNonEmpty(p.getEmailBlobUrlText()) ||
                         isNonEmpty(p.getPdfMobstatFileUrl()))
            .map(SummaryProcessedFile::getAccountNumber)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    metadata.setTotalCustomersProcessed(uniqueCustomers.size());

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
    payload.setMetadata(metadata);

    // Print files (.ps only)
    payload.setPrintFiles(printFiles.stream()
            .filter(pf -> pf.getPrintFileURL() != null && pf.getPrintFileURL().endsWith(".ps"))
            .peek(pf -> pf.setPrintStatus("SUCCESS"))
            .collect(Collectors.toList()));

    return payload;
}

// -------------------- buildProcessedFileEntries --------------------
private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    List<ProcessedFileEntry> allEntries = new ArrayList<>();
    for (SummaryProcessedFile file : processedFiles) {
        if (file == null) continue;

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());
        entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        entry.setEmailHtmlBlobUrl(file.getEmailBlobUrlHtml());
        entry.setEmailTextBlobUrl(file.getEmailBlobUrlText());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());

        // Statuses
        entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" : "FAILED");
        entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" : "FAILED");
        entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" : "FAILED");
        entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" : "FAILED");

        // Overall status
        if (("SUCCESS".equals(entry.getArchiveStatus()) &&
             ("SUCCESS".equals(entry.getEmailStatus()) ||
              "SUCCESS".equals(entry.getMobstatStatus()) ||
              "SUCCESS".equals(entry.getPrintStatus())))) {
            entry.setOverallStatus("SUCCESS");
        } else if ("SUCCESS".equals(entry.getArchiveStatus())) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }

        allEntries.add(entry);
    }
    return allEntries;
}

// -------------------- Utility --------------------
private static boolean isNonEmpty(String value) {
    return value != null && !value.trim().isEmpty();
}
