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

    // Walk all files and upload to blob
    try (Stream<Path> stream = Files.walk(jobDir)) {
        stream.filter(Files::isRegularFile).forEach(file -> {
            if (!Files.exists(file)) return;

            String fileName = file.getFileName().toString().toLowerCase();
            String parentFolder = file.getParent().getFileName().toString().toLowerCase();

            boolean allowed = false;
            if (parentFolder.contains(FOLDER_ARCHIVE) || parentFolder.contains(FOLDER_MOBSTAT) || parentFolder.contains(FOLDER_PRINT)) {
                allowed = fileName.endsWith(".pdf") || fileName.endsWith(".ps");
            } else if (parentFolder.contains(FOLDER_EMAIL)) {
                allowed = fileName.endsWith(".pdf") || fileName.endsWith(".html") || fileName.endsWith(".txt") || fileName.endsWith(".text");
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

    // Build final processed file list
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

        // 1️⃣ Archive entry (always added)
        SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, archiveEntry);
        archiveEntry.setArchiveBlobUrl(archivesForAccount.isEmpty() ? null : archivesForAccount.values().iterator().next());
        archiveEntry.setArchiveStatus(archiveEntry.getArchiveBlobUrl() != null ? "SUCCESS" : null);
        archiveEntry.setOverallStatus("SUCCESS");
        finalList.add(archiveEntry);

        // 2️⃣ Email entries (one row per file)
        for (Map.Entry<String, String> e : emailsForAccount.entrySet()) {
            String fname = e.getKey();
            String url = e.getValue();
            SummaryProcessedFile emailEntry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, emailEntry);

            if (fname.endsWith(".pdf")) emailEntry.setPdfEmailFileUrl(url);
            else if (fname.endsWith(".html")) emailEntry.setHtmlEmailFileUrl(url);
            else if (fname.endsWith(".txt") || fname.endsWith(".text")) emailEntry.setTextEmailFileUrl(url);

            emailEntry.setEmailStatus("SUCCESS");
            emailEntry.setOverallStatus("SUCCESS");
            finalList.add(emailEntry);
        }

        // 3️⃣ Print entries (only .ps)
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

        // 4️⃣ Mobstat entries
        for (Map.Entry<String, String> m : mobstatsForAccount.entrySet()) {
            SummaryProcessedFile mobEntry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, mobEntry);
            mobEntry.setPdfMobstatFileUrl(m.getValue());
            mobEntry.setPdfMobstatStatus("SUCCESS");
            mobEntry.setOverallStatus("SUCCESS");
            finalList.add(mobEntry);
        }
    }

    // Attach printFiles to KafkaMessage for passing to buildPayload
    msg.setPrintFiles(printFiles);

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

private static Metadata buildMetadata(
        List<ProcessedFileEntry> processedFileEntries, 
        String batchId, 
        String fileName, 
        KafkaMessage kafkaMessage) {

    Metadata metadata = new Metadata();

    // Count customers with at least one uploaded file
    Set<String> customersWithFiles = processedFileEntries.stream()
            .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl()) ||
                          isNonEmpty(pf.getEmailBlobUrl()) ||
                          isNonEmpty(pf.getPrintBlobUrl()) ||
                          isNonEmpty(pf.getMobstatBlobUrl()))
            .map(ProcessedFileEntry::getAccountNumber) // use account or customerId as unique
            .collect(Collectors.toSet());

    metadata.setTotalCustomersProcessed(customersWithFiles.size());

    // Determine overall processing status
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
            batchId, fileName, customersWithFiles.size(), overallStatus);

    return metadata;
}
