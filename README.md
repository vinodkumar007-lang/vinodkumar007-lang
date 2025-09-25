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

        // Populate header metadata
        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        // Build processed file entries from summary list
        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap, printFiles);
        payload.setProcessedFileList(processedFileEntries);

        // Total unique file URLs (email, print, mobstat, archive)
        int totalUniqueFiles = (int) processedList.stream()
                .flatMap(entry -> Stream.of(
                        entry.getEmailBlobUrlPdf(),
                        entry.getEmailBlobUrlHtml(),
                        entry.getEmailBlobUrlText(),
                        entry.getPdfMobstatFileUrl(),
                        entry.getArchiveBlobUrl()
                ))
                .filter(url -> url != null && !url.isBlank()) // ✅ only include actually available files
                .distinct()
                .count();

        // Populate payload details from KafkaMessage
        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
        payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
        payloadInfo.setEventID(kafkaMessage.getEventID());
        payloadInfo.setEventType(kafkaMessage.getEventType());
        payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
        payloadInfo.setFileCount(totalUniqueFiles);
        payload.setPayload(payloadInfo);

        // Metadata: count distinct customers and determine final status
        Metadata metadata = new Metadata();

        // Count total customers based on archive entries only
        long totalArchiveEntries = processedFileEntries.stream()
                .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl())).distinct()
                .count();
        metadata.setTotalCustomersProcessed((int) totalArchiveEntries);

        Set<String> statuses = processedFileEntries.stream()
                .map(ProcessedFileEntry::getOverallStatus)
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
        int customerCount = kafkaMessage.getBatchFiles().stream()
                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                .mapToInt(BatchFile::getCustomerCount)
                .sum();
        metadata.setCustomerCount(customerCount);
        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        // ✅ Step 1: Assign status based on conditions before decoding
        for (PrintFile pf : printFiles) {
            String psUrl = pf.getPrintFileURL();

            if (psUrl != null && psUrl.endsWith(".ps")) {
                // .ps file exists, set SUCCESS
                pf.setPrintStatus("SUCCESS");
            } else if (psUrl != null && errorMap.containsKey(psUrl)) {
                // Error found for this file
                pf.setPrintStatus("FAILED");
            } else {
                // Not found or unclear
                pf.setPrintStatus("");
            }
        }

// ✅ Step 2: Decode and collect into final printFileList
        List<PrintFile> printFileList = new ArrayList<>();

        for (PrintFile pf : printFiles) {
            if (pf.getPrintFileURL() != null && pf.getPrintFileURL().toLowerCase().endsWith(".ps")) {
                String decodedUrl = URLDecoder.decode(pf.getPrintFileURL(), StandardCharsets.UTF_8);

                PrintFile printFile = new PrintFile();
                printFile.setPrintFileURL(decodedUrl);
                printFile.setPrintStatus(pf.getPrintStatus() != null ? pf.getPrintStatus() : "");

                printFileList.add(printFile);
            }
        }

// ✅ Step 3: Set in payload
        payload.setPrintFiles(printFileList);

        return payload;
    }

    /**
     * Groups SummaryProcessedFile list into ProcessedFileEntry list by customer/account,
     * maps delivery statuses, and assigns overall status.
     *
     * @param processedFiles List of files that were processed
     * @param errorMap Map of errors for each delivery method
     * @return List of grouped ProcessedFileEntry objects with status and blob URLs
     */
    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> ignoredPrintFiles) {

        List<ProcessedFileEntry> allEntries = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>();

        for (SummaryProcessedFile file : processedFiles) {
            if (file == null) continue;

            String key = file.getCustomerId() + "|" + file.getAccountNumber() + "|" +
                    (file.getArchiveBlobUrl() != null ? new File(file.getArchiveBlobUrl()).getName() : "");
            if (uniqueKeys.contains(key)) continue;
            uniqueKeys.add(key);

            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());
            entry.setArchiveBlobUrl(file.getArchiveBlobUrl());
            entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
            entry.setPrintBlobUrl(file.getPrintFileUrl());

            // ✅ set email blob urls separately
            entry.setEmailBlobUrlPdf(file.getEmailBlobUrlPdf());
            entry.setEmailBlobUrlHtml(file.getEmailBlobUrlHtml());
            entry.setEmailBlobUrlText(file.getEmailBlobUrlText());

            // --- Ensure account is correct ---
            String account = file.getAccountNumber();
            if ((account == null || account.isBlank()) && isNonEmpty(file.getArchiveBlobUrl())) {
                account = extractAccountFromFileName(new File(file.getArchiveBlobUrl()).getName());
            }

            Map<String, String> errors = errorMap.getOrDefault(account, Collections.emptyMap());

            // --- Email status if any of pdf/html/text present ---
            entry.setEmailStatus(
                    isNonEmpty(file.getEmailBlobUrlPdf()) ||
                            isNonEmpty(file.getEmailBlobUrlHtml()) ||
                            isNonEmpty(file.getEmailBlobUrlText()) ? "SUCCESS" :
                            "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : ""
            );

            entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
            entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
            entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

            // --- Determine overall status ---
            boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
            boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
            boolean printSuccess = "SUCCESS".equals(entry.getPrintStatus());
            boolean archiveSuccess = "SUCCESS".equals(entry.getArchiveStatus());

            if ((emailSuccess && archiveSuccess) ||
                    (mobstatSuccess && archiveSuccess && !emailSuccess && !printSuccess) ||
                    (printSuccess && archiveSuccess && !emailSuccess && !mobstatSuccess)) {
                entry.setOverallStatus("SUCCESS");
            } else if (archiveSuccess) {
                entry.setOverallStatus("PARTIAL");
            } else {
                entry.setOverallStatus("FAILED");
            }

            if (errorMap.containsKey(account) && !"FAILED".equals(entry.getOverallStatus())) {
                entry.setOverallStatus("PARTIAL");
            }

            allEntries.add(entry);
        }

        return allEntries;
    }
