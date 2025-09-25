
    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();
        if (jobDir == null || customerList == null || msg == null) return finalList;

        // ✅ Maps for each type
        Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
        Map<String, Map<String, String>> accountToEmailFiles = new HashMap<>();
        Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
        Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();

        // -------- 🔍 Walk ALL folders inside jobDir --------
        try (Stream<Path> stream = Files.walk(jobDir)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                if (!Files.exists(file)) {
                    logger.warn("[{}] ⏩ Skipping missing file: {}", msg.getBatchId(), file);
                    return;
                }

                String fileName = file.getFileName().toString().toLowerCase();
                String parentFolder = file.getParent().getFileName().toString().toLowerCase();

                // ✅ Only allow PDF and PS files
                if (!(fileName.endsWith(".pdf") || fileName.endsWith(".ps"))) {
                    logger.debug("[{}] ⏩ Skipping non-pdf/ps file: {}", msg.getBatchId(), fileName);
                    return;
                }

                try {
                    String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), parentFolder, msg));

                    // ✅ Match file to customers by account number
                    for (SummaryProcessedFile customer : customerList) {
                        if (customer == null || customer.getAccountNumber() == null) continue;
                        String account = customer.getAccountNumber();
                        if (!fileName.contains(account)) continue;

                        if (parentFolder.contains("archive")) {
                            accountToArchiveFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 📦 Uploaded archive file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);
                        } else if (parentFolder.contains("email")) {
                            accountToEmailFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 📧 Uploaded email file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);
                        } else if (parentFolder.contains("mobstat")) {
                            accountToMobstatFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 📱 Uploaded mobstat file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);
                        } else if (parentFolder.contains("print")) {
                            accountToPrintFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 🖨 Uploaded print file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);
                        } else {
                            logger.info("[{}] ℹ️ Ignoring file (unmapped folder) {} in {}", msg.getBatchId(), fileName, parentFolder);
                        }
                    }

                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                }
            });
        }

        // -------- Build final list (fixed combinations) --------
        Set<String> uniqueKeys = new HashSet<>();
        for (SummaryProcessedFile customer : customerList) {
            if (customer == null || customer.getAccountNumber() == null) continue;
            String account = customer.getAccountNumber();

            Map<String, String> archivesForAccount = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
            Map<String, String> emailsForAccount = accountToEmailFiles.getOrDefault(account, Collections.emptyMap());
            Map<String, String> mobstatsForAccount = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());

            // Skip if nothing exists
            if (archivesForAccount.isEmpty() && emailsForAccount.isEmpty() && mobstatsForAccount.isEmpty()) continue;

            List<String> archiveFiles = new ArrayList<>(archivesForAccount.keySet());
            if (archiveFiles.isEmpty()) archiveFiles.add(null);

            List<String> emailFiles = new ArrayList<>(emailsForAccount.values());
            if (emailFiles.isEmpty()) emailFiles.add(null);

            List<String> mobstatFiles = new ArrayList<>(mobstatsForAccount.values());
            if (mobstatFiles.isEmpty()) mobstatFiles.add(null);

            // ✅ Iterate over all combinations
            for (String archiveFileName : archiveFiles) {
                for (String emailUrl : emailFiles) {
                    for (String mobstatUrl : mobstatFiles) {
                        String key = customer.getCustomerId() + "|" + account + "|" +
                                (archiveFileName != null ? archiveFileName : "noArchive") + "|" +
                                (emailUrl != null ? emailUrl : "noEmail") + "|" +
                                (mobstatUrl != null ? mobstatUrl : "noMobstat");
                        if (uniqueKeys.contains(key)) continue;
                        uniqueKeys.add(key);

                        SummaryProcessedFile entry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, entry);
                        entry.setArchiveBlobUrl(archiveFileName != null ? archivesForAccount.get(archiveFileName) : null);
                        entry.setPdfEmailFileUrl(emailUrl);
                        entry.setPdfMobstatFileUrl(mobstatUrl);
                        finalList.add(entry);
                    }
                }
            }
        }
        
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
                        entry.getPdfEmailFileUrl(),
                        entry.getPdfMobstatFileUrl(),
                        entry.getArchiveBlobUrl()
                ))
                .filter(Objects::nonNull)
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
            if (pf.getPrintFileURL() != null) {
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
        Set<String> uniqueKeys = new HashSet<>(); // customerId + accountNumber + archiveFilename

        for (SummaryProcessedFile file : processedFiles) {
            if (file == null) continue;

            String key = file.getCustomerId() + "|" + file.getAccountNumber() + "|" +
                    (file.getArchiveBlobUrl() != null ? new File(file.getArchiveBlobUrl()).getName() : "");
            if (uniqueKeys.contains(key)) continue;
            uniqueKeys.add(key);

            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());
            entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
            entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
            entry.setPrintBlobUrl(file.getPrintFileUrl());
            entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

            // --- Ensure we get the correct account number from filename if needed ---
            String account = file.getAccountNumber();
            if ((account == null || account.isBlank()) && isNonEmpty(file.getArchiveBlobUrl())) {
                account = extractAccountFromFileName(new File(file.getArchiveBlobUrl()).getName());
            }

            Map<String, String> errors = errorMap.getOrDefault(account, Collections.emptyMap());

            // --- Set individual statuses ---
            entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
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

            // --- If any errors exist for this account, mark as PARTIAL if not FAILED ---
            if (errorMap.containsKey(account) && !"FAILED".equals(entry.getOverallStatus())) {
                entry.setOverallStatus("PARTIAL");
            }

            allEntries.add(entry);
        }

        return allEntries;
    }

    public static String extractAccountFromFileName(String fileName) {
        if (fileName == null) return null;

        // Regex: first sequence of digits either before or after underscore
        Matcher matcher = Pattern.compile("(?:^|_)(\\d+)").matcher(fileName);
        if (matcher.find()) {
            return matcher.group(1); // first numeric sequence found
        }

        logger.warn("⚠️ Could not extract account from filename: {}", fileName);
        return null;
    }
