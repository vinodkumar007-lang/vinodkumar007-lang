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

private Map<String, String> emailBlobUrls; // keys: pdf, html, text

public Map<String, String> getEmailBlobUrls() { return emailBlobUrls; }
public void setEmailBlobUrls(Map<String, String> emailBlobUrls) { this.emailBlobUrls = emailBlobUrls; }

private String emailStatus;
public String getEmailStatus() { return emailStatus; }
public void setEmailStatus(String emailStatus) { this.emailStatus = emailStatus; }


private static ProcessedFileEntry mapToProcessedFileEntry(SummaryProcessedFile file, Map<String, String> errors) {
    ProcessedFileEntry entry = new ProcessedFileEntry();
    entry.setCustomerId(file.getCustomerId());
    entry.setAccountNumber(file.getAccountNumber());
    entry.setArchiveBlobUrl(file.getArchiveBlobUrl());
    entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
    entry.setPrintBlobUrl(file.getPrintFileUrl());

    Map<String, String> emails = file.getEmailBlobUrls() != null ? file.getEmailBlobUrls() : Collections.emptyMap();
    entry.setEmailBlobUrlPdf(emails.get("pdf"));
    entry.setEmailBlobUrlHtml(emails.get("html"));
    entry.setEmailBlobUrlText(emails.get("text"));

    entry.setEmailStatus(!emails.isEmpty() ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");

    entry.setMobstatStatus(isNonEmpty(entry.getMobstatBlobUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
    entry.setPrintStatus(isNonEmpty(entry.getPrintBlobUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
    entry.setArchiveStatus(isNonEmpty(entry.getArchiveBlobUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

    return entry;
}
