private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();
        if (jobDir == null || customerList == null || msg == null) return finalList;

        // Maps for each type
        Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
        Map<String, Map<String, String>> accountToEmailFiles = new HashMap<>();
        Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
        Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();

        // Walk all files
        try (Stream<Path> stream = Files.walk(jobDir)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                if (!Files.exists(file)) return;

                String fileName = file.getFileName().toString().toLowerCase();
                String parentFolder = file.getParent().getFileName().toString().toLowerCase();

                // Dynamically allow files
                boolean allowed = false;
                if (parentFolder.contains(FOLDER_ARCHIVE) || parentFolder.contains(FOLDER_MOBSTAT) || parentFolder.contains(FOLDER_PRINT)) {
                    allowed = fileName.endsWith(".pdf") || fileName.endsWith(".ps");
                } else if (parentFolder.contains(FOLDER_EMAIL)) {
                    allowed = fileName.endsWith(".pdf") || fileName.endsWith(".html") || fileName.endsWith(".txt") || fileName.endsWith(".text");
                }

                if (!allowed) {
                    logger.debug("[{}] ⏩ Skipping unsupported file: {}", msg.getBatchId(), fileName);
                    return;
                }

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

        // Build final list
        Set<String> uniqueKeys = new HashSet<>();
        for (SummaryProcessedFile customer : customerList) {
            if (customer == null || customer.getAccountNumber() == null) continue;
            String account = customer.getAccountNumber();

            Map<String, String> archivesForAccount = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
            Map<String, String> emailsForAccount = accountToEmailFiles.getOrDefault(account, Collections.emptyMap());
            Map<String, String> mobstatsForAccount = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());
            Map<String, String> printsForAccount = accountToPrintFiles.getOrDefault(account, Collections.emptyMap());

            // Skip if nothing exists
            if (archivesForAccount.isEmpty() && emailsForAccount.isEmpty() && mobstatsForAccount.isEmpty() && printsForAccount.isEmpty()) continue;

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);

            // Archive, Mobstat, Print
            entry.setArchiveBlobUrl(archivesForAccount.isEmpty() ? null : archivesForAccount.values().iterator().next());
            entry.setPdfMobstatFileUrl(mobstatsForAccount.isEmpty() ? null : mobstatsForAccount.values().iterator().next());
            entry.setPrintFileUrl(printsForAccount.isEmpty() ? null : printsForAccount.values().iterator().next());

            // Email URLs by type
            Map<String, String> emailMap = new HashMap<>();
            for (Map.Entry<String, String> e : emailsForAccount.entrySet()) {
                String fname = e.getKey();
                if (fname.endsWith(".pdf")) emailMap.put("pdf", e.getValue());
                else if (fname.endsWith(".html")) emailMap.put("html", e.getValue());
                else if (fname.endsWith(".txt") || fname.endsWith(".text")) emailMap.put("text", e.getValue());
            }
            entry.setEmailBlobUrls(emailMap);

            // Single email status
            boolean anyEmailExists = !emailMap.isEmpty();
            entry.setEmailStatus(anyEmailExists ? "SUCCESS" : null);

            String uniqueKey = customer.getCustomerId() + "|" + account;
            if (!uniqueKeys.contains(uniqueKey)) {
                uniqueKeys.add(uniqueKey);
                finalList.add(entry);
            }
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }


    public static String writeSummaryJsonToFile(SummaryPayload payload) {
        if (payload == null) {
            logger.error("❌ SummaryPayload is null. Cannot write summary.json.");
            throw new IllegalArgumentException("SummaryPayload cannot be null");
        }

        try {
            if (payload.getHeader() == null) {
                logger.warn("⚠️ SummaryPayload.header is null.");
            }
            if (payload.getMetadata() == null) {
                logger.warn("⚠️ SummaryPayload.metadata is null.");
            }
            if (payload.getProcessedFiles() == null || payload.getProcessedFiles().isEmpty()) {
                logger.warn("⚠️ No processedFiles found in payload.");
            }

            String batchId = Optional.ofNullable(payload.getBatchID()).orElse("unknown");
            String fileName = "summary_" + batchId + ".json";

            Path tempDir = Files.createTempDirectory("summaryFiles");
            Path summaryFilePath = tempDir.resolve(fileName);

            File summaryFile = summaryFilePath.toFile();
            if (summaryFile.exists()) {
                Files.delete(summaryFilePath);
                logger.warn("Existing summary file deleted: {}", summaryFilePath);
            }

            // ✅ Write JSON
            objectMapper.writeValue(summaryFile, payload);
            logger.info("✅ Summary JSON written successfully at: {}", summaryFilePath);

            return summaryFilePath.toAbsolutePath().toString();

        } catch (Exception e) {
            logger.error("❌ Failed to write summary.json", e);
            throw new RuntimeException("Failed to write summary JSON", e);
        }
    }

    /**
     * Constructs a SummaryPayload object from various input values.
     *
     * @param kafkaMessage Kafka input message object
     * @param processedList List of processed file entries
     * @param fileName Output summary file name
     * @param batchId Batch identifier
     * @param timestamp Timestamp string
     * @param errorMap Map of errors keyed by account and delivery method
     * @param printFiles List of print file URLs
     * @return SummaryPayload object
     */
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
        Metadata metadata = buildMetadata(processedFileEntries, batchId, fileName, kafkaMessage);
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

    private static Metadata buildMetadata(List<ProcessedFileEntry> processedFileEntries, String batchId, String fileName, KafkaMessage kafkaMessage) {
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
        int customerCount = kafkaMessage.getBatchFiles().stream()
                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                .mapToInt(BatchFile::getCustomerCount)
                .sum();
        metadata.setCustomerCount(customerCount);
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

            // ✅ Only include .ps files
            if (psUrl == null || !psUrl.toLowerCase().endsWith(".ps")) {
                logger.debug("[buildPayload] Skipping non-.ps PrintFile. batchId={}, fileName={}, url={}", batchId, fileName, psUrl);
                continue;
            }

            String decodedUrl = URLDecoder.decode(psUrl, StandardCharsets.UTF_8);

            PrintFile printFile = new PrintFile();
            printFile.setPrintFileURL(decodedUrl);
            printFile.setPrintStatus("SUCCESS"); // Set status SUCCESS by default; adjust if errorMap check needed

            result.add(printFile);

            logger.debug("[GT] PrintFile processed. batchId={}, fileName={}, psUrl={}, status={}",
                    batchId, fileName, decodedUrl, printFile.getPrintStatus());
        }

        return result;
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

        // --- Validate inputs ---
        processedFiles = validateProcessedFiles(processedFiles, ignoredPrintFiles);
        if (processedFiles.isEmpty()) return Collections.emptyList();
        if (errorMap == null) {
            logger.warn("[buildProcessedFileEntries] errorMap is null. Using empty map.");
            errorMap = Collections.emptyMap();
        }

        logger.info("[buildProcessedFileEntries] Start building entries. processedFilesCount={}", processedFiles.size());

        List<ProcessedFileEntry> allEntries = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>();

        for (SummaryProcessedFile file : processedFiles) {
            if (file == null) {
                logger.debug("[buildProcessedFileEntries] Skipping null SummaryProcessedFile.");
                continue;
            }

            String archiveFileName = extractArchiveFileName(file);
            String key = generateUniqueKey(file, archiveFileName);

            if (!uniqueKeys.add(key)) {
                logger.debug("[GT] Duplicate skipped. customerId={}, account={}, archiveFile={}",
                        file.getCustomerId(), file.getAccountNumber(), archiveFileName);
                continue;
            }

            String account = getAccount(file, archiveFileName);

            Map<String, String> errors = errorMap.getOrDefault(account, Collections.emptyMap());

            ProcessedFileEntry entry = mapToProcessedFileEntry(file, errors);
            entry.setOverallStatus(determineOverallStatus(entry, account, errorMap));

            logger.info("[GT] customerId={}, account={}, archiveFile={} | email={}, mobstat={}, print={}, archive={}, overall={}",
                    entry.getCustomerId(), account, archiveFileName,
                    entry.getEmailStatus(), entry.getMobstatStatus(), entry.getPrintStatus(),
                    entry.getArchiveStatus(), entry.getOverallStatus());

            allEntries.add(entry);
        }

        logger.info("[buildProcessedFileEntries] Completed. totalEntries={}", allEntries.size());
        return allEntries;
    }

// --- Helper Methods ---

    private static List<SummaryProcessedFile> validateProcessedFiles(List<SummaryProcessedFile> processedFiles, List<PrintFile> ignoredPrintFiles) {
        if (processedFiles == null || processedFiles.isEmpty()) {
            logger.warn("[buildProcessedFileEntries] processedFiles is null/empty. Returning empty list.");
            if (ignoredPrintFiles != null && !ignoredPrintFiles.isEmpty()) {
                logger.debug("[buildProcessedFileEntries] ignoredPrintFiles present but not used. count={}", ignoredPrintFiles.size());
            }
            return Collections.emptyList();
        }
        if (ignoredPrintFiles == null) {
            logger.debug("[buildProcessedFileEntries] ignoredPrintFiles is null.");
        } else if (!ignoredPrintFiles.isEmpty()) {
            logger.debug("[buildProcessedFileEntries] ignoredPrintFiles provided (not used). count={}", ignoredPrintFiles.size());
        }
        return processedFiles;
    }

    private static String extractArchiveFileName(SummaryProcessedFile file) {
        return file.getArchiveBlobUrl() != null ? new java.io.File(file.getArchiveBlobUrl()).getName() : "";
    }

    private static String generateUniqueKey(SummaryProcessedFile file, String archiveFileName) {
        return file.getCustomerId() + "|" + file.getAccountNumber() + "|" + archiveFileName;
    }

    private static String getAccount(SummaryProcessedFile file, String archiveFileName) {
        String account = file.getAccountNumber();
        if ((account == null || account.isBlank()) && isNonEmpty(file.getArchiveBlobUrl())) {
            account = extractAccountFromFileName(archiveFileName);
            logger.debug("[GT] Account missing. Extracted from archive file. customerId={}, extractedAccount={}, archiveFile={}",
                    file.getCustomerId(), account, archiveFileName);
        }
        return account;
    }

    private static ProcessedFileEntry mapToProcessedFileEntry(SummaryProcessedFile file, Map<String, String> errors) {
        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());
        entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

        entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
        entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

        return entry;
    }

    private static String determineOverallStatus(ProcessedFileEntry entry, String account, Map<String, Map<String, String>> errorMap) {
        boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
        boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
        boolean printSuccess  = "SUCCESS".equals(entry.getPrintStatus());
        boolean archiveSuccess= "SUCCESS".equals(entry.getArchiveStatus());

        String overallStatus;
        if ((emailSuccess && archiveSuccess) ||
                (mobstatSuccess && archiveSuccess && !emailSuccess && !printSuccess) ||
                (printSuccess && archiveSuccess && !emailSuccess && !mobstatSuccess)) {
            overallStatus = "SUCCESS";
        } else if (archiveSuccess) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "FAILED";
        }

        if (errorMap.containsKey(account) && !"FAILED".equals(overallStatus)) {
            overallStatus = "PARTIAL";
        }

        return overallStatus;
    }

    public static String extractAccountFromFileName(String fileName) {
        if (fileName == null || !fileName.contains("_")) return null;
        return fileName.split("_")[0];
    }

    /**
     * Utility method to check if a string is non-null and non-blank.
     *
     * @param value The input string
     * @return true if not null or blank
     */
    private static boolean isNonEmpty(String value) {

        return value != null && !value.trim().isEmpty();
    }
}
