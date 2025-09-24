private static Metadata buildMetadata(List<ProcessedFileEntry> processedFileEntries, String batchId, String fileName, KafkaMessage kafkaMessage) {
    Metadata metadata = new Metadata();

    // ✅ Count all customers who have at least one file OR even empty customer row
    Set<String> customersWithFiles = processedFileEntries.stream()
            .map(ProcessedFileEntry::getAccountNumber)
            .filter(Objects::nonNull)
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

    logger.info("[METADATA] batchId={}, fileName={}, totalCustomers={}, customerCount={}, status={}",
            batchId, fileName, customersWithFiles.size(), customerCount, overallStatus);

    return metadata;
}

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

    // Step 1: Upload & categorize files
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
                allowed = lowerFileName.endsWith(".pdf") || lowerFileName.endsWith(".html") ||
                        lowerFileName.endsWith(".txt") || lowerFileName.endsWith(".text");
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

    // Step 2: Build final list — include every customer at least once
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        SummaryProcessedFile entry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, entry);

        // Archive
        Map<String, String> archivesForAccount = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
        if (!archivesForAccount.isEmpty()) {
            entry.setArchiveBlobUrl(archivesForAccount.values().iterator().next());
            entry.setArchiveStatus("SUCCESS");
        }

        // Email
        Map<String, String> emailsForAccount = accountToEmailFiles.getOrDefault(account, Collections.emptyMap());
        for (Map.Entry<String, String> e : emailsForAccount.entrySet()) {
            String fname = e.getKey();
            String url = e.getValue();
            if (fname.toLowerCase().endsWith(".pdf")) entry.setEmailBlobUrlPdf(url);
            else if (fname.toLowerCase().endsWith(".html")) entry.setEmailBlobUrlHtml(url);
            else if (fname.toLowerCase().endsWith(".txt") || fname.toLowerCase().endsWith(".text"))
                entry.setEmailBlobUrlText(url);
        }
        if (!emailsForAccount.isEmpty()) entry.setEmailStatus("SUCCESS");

        // Mobstat
        Map<String, String> mobstatsForAccount = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());
        if (!mobstatsForAccount.isEmpty()) {
            entry.setPdfMobstatFileUrl(mobstatsForAccount.values().iterator().next());
            entry.setPdfMobstatStatus("SUCCESS");
        }

        entry.setOverallStatus("SUCCESS");
        finalList.add(entry);

        // Print (.ps only)
        Map<String, String> printsForAccount = accountToPrintFiles.getOrDefault(account, Collections.emptyMap());
        for (Map.Entry<String, String> p : printsForAccount.entrySet()) {
            String fname = p.getKey();
            if (!fname.toLowerCase().endsWith(".ps")) continue;

            SummaryProcessedFile printEntry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, printEntry);
            printEntry.setPrintFileUrl(p.getValue());
            printEntry.setPrintStatus("SUCCESS");
            printEntry.setOverallStatus("SUCCESS");
            finalList.add(printEntry);

            PrintFile pf = new PrintFile();
            pf.setPrintFileURL(p.getValue());
            pf.setPrintStatus("SUCCESS");
            printFiles.add(pf);
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}
