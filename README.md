private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

    // Maps for each type
    Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailPdfFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailHtmlFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailTxtFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();

    try (Stream<Path> stream = Files.walk(jobDir)) {
        stream.filter(Files::isRegularFile).forEach(file -> {
            if (!Files.exists(file)) return;

            String fileName = file.getFileName().toString();
            String fileNameLower = fileName.toLowerCase();
            String parentFolder = file.getParent().getFileName().toString().toLowerCase();

            try {
                String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), parentFolder, msg));

                for (SummaryProcessedFile customer : customerList) {
                    if (customer == null || customer.getAccountNumber() == null) continue;
                    String account = customer.getAccountNumber();
                    if (!fileName.contains(account)) continue;

                    if (parentFolder.contains("archive")) {
                        accountToArchiveFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains("email")) {
                        if (fileNameLower.endsWith(".pdf")) {
                            accountToEmailPdfFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        } else if (fileNameLower.endsWith(".html")) {
                            accountToEmailHtmlFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        } else if (fileNameLower.endsWith(".txt")) {
                            accountToEmailTxtFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        }
                    } else if (parentFolder.contains("mobstat")) {
                        accountToMobstatFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains("print")) {
                        if (fileNameLower.endsWith(".ps") && !fileName.contains("Print_Report")) {
                            accountToPrintFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("[{}] Failed to upload file {}: {}", msg.getBatchId(), fileName, e.getMessage());
            }
        });
    }

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        SummaryProcessedFile entry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, entry);

        // Set Email URLs separately
        Map<String, String> pdfMap = accountToEmailPdfFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> htmlMap = accountToEmailHtmlFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> txtMap = accountToEmailTxtFiles.getOrDefault(account, Collections.emptyMap());

        entry.setEmailBlobUrlPdf(pdfMap.isEmpty() ? null : pdfMap.values().iterator().next());
        entry.setEmailBlobUrlHtml(htmlMap.isEmpty() ? null : htmlMap.values().iterator().next());
        entry.setEmailBlobUrlTxt(txtMap.isEmpty() ? null : txtMap.values().iterator().next());

        // Archive & Mobstat URLs
        entry.setArchiveBlobUrl(accountToArchiveFiles.getOrDefault(account, Collections.emptyMap())
                .values().stream().findFirst().orElse(null));
        entry.setPdfMobstatFileUrl(accountToMobstatFiles.getOrDefault(account, Collections.emptyMap())
                .values().stream().findFirst().orElse(null));

        // Print files (only .ps, excluding Print_Report)
        Map<String, String> printMap = accountToPrintFiles.getOrDefault(account, Collections.emptyMap());
        entry.setPrintFileUrl(printMap.isEmpty() ? null : printMap.values().stream().findFirst().orElse(null));

        // Determine overallStatus
        boolean hasArchive = entry.getArchiveBlobUrl() != null;
        boolean hasEmail = entry.getEmailBlobUrlPdf() != null || entry.getEmailBlobUrlHtml() != null || entry.getEmailBlobUrlTxt() != null;
        boolean hasMobstat = entry.getPdfMobstatFileUrl() != null;
        boolean hasPrint = entry.getPrintFileUrl() != null;

        if (hasArchive && (hasEmail || hasMobstat || hasPrint)) entry.setOverallStatus("SUCCESS");
        else if (hasArchive) entry.setOverallStatus("PARTIAL");
        else entry.setOverallStatus("FAILED");

        finalList.add(entry);
    }

    logger.info("[{}] buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
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
    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);

    // Header metadata
    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setTimestamp(timestamp);
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setProduct(kafkaMessage.getSourceSystem());
    header.setJobName(kafkaMessage.getSourceSystem());
    payload.setHeader(header);

    // Build processed file entries
    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap, printFiles);
    payload.setProcessedFileList(processedFileEntries);

    // Total unique files count (archive + emails + mobstat)
    int totalUniqueFiles = (int) processedList.stream()
            .flatMap(entry -> Stream.of(
                    entry.getEmailBlobUrlPdf(),
                    entry.getEmailBlobUrlHtml(),
                    entry.getEmailBlobUrlTxt(),
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
    long totalArchiveEntries = processedFileEntries.stream()
            .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl()))
            .distinct()
            .count();
    metadata.setTotalCustomersProcessed((int) totalArchiveEntries);

    Set<String> statuses = processedFileEntries.stream()
            .map(ProcessedFileEntry::getOverallStatus)
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

    // Handle PrintFiles: only .ps files
    List<PrintFile> finalPrintFiles = printFiles.stream()
            .filter(pf -> pf.getPrintFileURL() != null && pf.getPrintFileURL().endsWith(".ps"))
            .map(pf -> {
                PrintFile pfNew = new PrintFile();
                pfNew.setPrintFileURL(URLDecoder.decode(pf.getPrintFileURL(), StandardCharsets.UTF_8));
                pfNew.setPrintStatus(pf.getPrintStatus());
                return pfNew;
            })
            .collect(Collectors.toList());

    payload.setPrintFiles(finalPrintFiles);

    return payload;
}

/**
 * Groups SummaryProcessedFile list into ProcessedFileEntry list by customer/account,
 * maps delivery statuses (including separate email fields), and assigns overall status.
 */
private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> ignoredPrintFiles) {

    List<ProcessedFileEntry> allEntries = new ArrayList<>();
    Set<String> uniqueKeys = new HashSet<>();

    for (SummaryProcessedFile file : processedFiles) {
        if (file == null) continue;

        // Unique key by customer+account+archive
        String key = file.getCustomerId() + "|" + file.getAccountNumber() + "|" +
                (file.getArchiveBlobUrl() != null ? new File(file.getArchiveBlobUrl()).getName() : "");
        if (uniqueKeys.contains(key)) continue;
        uniqueKeys.add(key);

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());
        entry.setEmailBlobUrlPdf(file.getEmailBlobUrlPdf());
        entry.setEmailBlobUrlHtml(file.getEmailBlobUrlHtml());
        entry.setEmailBlobUrlTxt(file.getEmailBlobUrlTxt());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());

        // Determine overall status
        boolean hasArchive = isNonEmpty(entry.getArchiveBlobUrl());
        boolean hasEmail = isNonEmpty(entry.getEmailBlobUrlPdf()) || isNonEmpty(entry.getEmailBlobUrlHtml()) || isNonEmpty(entry.getEmailBlobUrlTxt());
        boolean hasMobstat = isNonEmpty(entry.getMobstatBlobUrl());
        boolean hasPrint = isNonEmpty(entry.getPrintBlobUrl());

        if (hasArchive && (hasEmail || hasMobstat || hasPrint)) entry.setOverallStatus("SUCCESS");
        else if (hasArchive) entry.setOverallStatus("PARTIAL");
        else entry.setOverallStatus("FAILED");

        allEntries.add(entry);
    }

    return allEntries;
}




