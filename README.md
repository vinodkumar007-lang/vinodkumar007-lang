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

        // --- Archive entry ---
        SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, archiveEntry);
        archiveEntry.setArchiveBlobUrl(archivesForAccount.isEmpty() ? null : archivesForAccount.values().iterator().next());
        archiveEntry.setArchiveStatus(archiveEntry.getArchiveBlobUrl() != null ? "SUCCESS" : null);

        // --- Combine all email URLs into the same entry ---
        if (!emailsForAccount.isEmpty()) {
            for (Map.Entry<String, String> e : emailsForAccount.entrySet()) {
                String fname = e.getKey();
                String url = e.getValue();
                if (fname.endsWith(".pdf")) archiveEntry.setPdfEmailFileUrl(url);
                else if (fname.endsWith(".html")) archiveEntry.setHtmlEmailFileUrl(url);
                else if (fname.endsWith(".txt") || fname.endsWith(".text")) archiveEntry.setTextEmailFileUrl(url);
            }
            archiveEntry.setEmailStatus("SUCCESS");
        }

        // --- Mobstat URL ---
        if (!mobstatsForAccount.isEmpty()) {
            archiveEntry.setPdfMobstatFileUrl(mobstatsForAccount.values().iterator().next());
            archiveEntry.setPdfMobstatStatus("SUCCESS");
        }

        archiveEntry.setOverallStatus("SUCCESS");
        finalList.add(archiveEntry);

        // --- Print entries (only .ps) ---
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

    // Attach printFiles to KafkaMessage for passing to buildPayload
    msg.setPrintFiles(printFiles);

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}
