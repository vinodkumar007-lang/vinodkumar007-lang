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
    Set<String> uniqueKeys = new HashSet<>();
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
        archiveEntry.setOverallStatus("SUCCESS"); // can also call calculateOverallStatus(archiveEntry)
        finalList.add(archiveEntry);

        // 2️⃣ Email entries (separate row per type)
        for (Map.Entry<String, String> e : emailsForAccount.entrySet()) {
            String fname = e.getKey();
            String url = e.getValue();
            SummaryProcessedFile emailEntry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, emailEntry);

            if (fname.endsWith(".pdf")) emailEntry.setEmailBlobUrlPdf(url);
            else if (fname.endsWith(".html")) emailEntry.setEmailBlobUrlHtml(url);
            else if (fname.endsWith(".txt") || fname.endsWith(".text")) emailEntry.setEmailBlobUrlText(url);

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

            // Add to printFiles array for summary.json
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
            mobEntry.setMobstatStatus("SUCCESS");
            mobEntry.setOverallStatus("SUCCESS");
            finalList.add(mobEntry);
        }
    }

    // You can now attach printFiles to summaryPayload elsewhere
    // summaryPayload.setPrintFiles(printFiles);

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}
