private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

    // Maps for each type per account
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

    // Grouped processed file per customer+account
    Map<String, SummaryProcessedFile> groupedMap = new LinkedHashMap<>();
    List<PrintFile> printFiles = new ArrayList<>();

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();
        String key = customer.getCustomerId() + "|" + account;

        SummaryProcessedFile grouped = groupedMap.getOrDefault(key, new SummaryProcessedFile());
        BeanUtils.copyProperties(customer, grouped);

        // --- Archive ---
        Map<String, String> archives = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
        if (!archives.isEmpty()) {
            grouped.setArchiveBlobUrl(archives.values().iterator().next());
            grouped.setArchiveStatus("SUCCESS");
        }

        // --- Email ---
        Map<String, String> emails = accountToEmailFiles.getOrDefault(account, Collections.emptyMap());
        for (Map.Entry<String, String> e : emails.entrySet()) {
            String fname = e.getKey();
            String url = e.getValue();
            if (fname.endsWith(".pdf")) grouped.setPdfEmailFileUrl(url);
            else if (fname.endsWith(".html")) grouped.setHtmlEmailFileUrl(url);
            else if (fname.endsWith(".txt") || fname.endsWith(".text")) grouped.setTextEmailFileUrl(url);
        }

        if (!emails.isEmpty()) grouped.setEmailStatus("SUCCESS");

        // --- Mobstat ---
        Map<String, String> mobstats = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());
        if (!mobstats.isEmpty()) {
            grouped.setPdfMobstatFileUrl(mobstats.values().iterator().next());
            grouped.setPdfMobstatStatus("SUCCESS");
        }

        // --- Print (.ps only) ---
        Map<String, String> prints = accountToPrintFiles.getOrDefault(account, Collections.emptyMap());
        for (Map.Entry<String, String> p : prints.entrySet()) {
            String fname = p.getKey();
            String url = p.getValue();
            if (!fname.toLowerCase().endsWith(".ps")) continue;

            grouped.setPrintFileUrl(url);
            grouped.setPrintStatus("SUCCESS");

            PrintFile pf = new PrintFile();
            pf.setPrintFileURL(url);
            pf.setPrintStatus("SUCCESS");
            printFiles.add(pf);
        }

        // --- Overall status ---
        if (grouped.getOverallStatus() == null) grouped.setOverallStatus("SUCCESS");

        groupedMap.put(key, grouped);
    }

    // Build final list
    finalList.addAll(groupedMap.values());

    // Attach printFiles to KafkaMessage for buildPayload
    msg.setPrintFiles(printFiles);

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final grouped list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}
