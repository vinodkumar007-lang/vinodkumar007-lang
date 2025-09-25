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

            // ✅ Only allow PDF, PS, HTML, TXT for processing
            if (!(fileName.endsWith(".pdf") || fileName.endsWith(".ps") ||
                  fileName.endsWith(".html") || fileName.endsWith(".txt"))) {
                logger.debug("[{}] ⏩ Skipping unsupported file: {}", msg.getBatchId(), fileName);
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
                        Map<String, String> fileMap = accountToEmailFiles.computeIfAbsent(account, k -> new HashMap<>());

                        if (fileName.endsWith(".pdf")) {
                            fileMap.put("PDF", url);
                        } else if (fileName.endsWith(".html")) {
                            fileMap.put("HTML", url);
                        } else if (fileName.endsWith(".txt")) {
                            fileMap.put("TEXT", url);
                        } else {
                            fileMap.put(fileName, url); // fallback
                        }
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

        // ✅ Email handled separately (pdf/html/text) → no need to expand here
        List<String> mobstatFiles = new ArrayList<>(mobstatsForAccount.values());
        if (mobstatFiles.isEmpty()) mobstatFiles.add(null);

        for (String archiveFileName : archiveFiles) {
            for (String mobstatUrl : mobstatFiles) {
                String key = customer.getCustomerId() + "|" + account + "|" +
                        (archiveFileName != null ? archiveFileName : "noArchive") + "|" +
                        (mobstatUrl != null ? mobstatUrl : "noMobstat");
                if (uniqueKeys.contains(key)) continue;
                uniqueKeys.add(key);

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);

                entry.setArchiveBlobUrl(archiveFileName != null ? archivesForAccount.get(archiveFileName) : null);

                // ✅ set email files separately
                entry.setEmailBlobUrlPdf(emailsForAccount.get("PDF"));
                entry.setEmailBlobUrlHtml(emailsForAccount.get("HTML"));
                entry.setEmailBlobUrlText(emailsForAccount.get("TEXT"));

                entry.setPdfMobstatFileUrl(mobstatUrl);

                finalList.add(entry);
            }
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}

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

for (PrintFile pf : printFiles) {
    if (pf.getPrintFileURL() != null && pf.getPrintFileURL().toLowerCase().endsWith(".ps")) {
        String decodedUrl = URLDecoder.decode(pf.getPrintFileURL(), StandardCharsets.UTF_8);

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL(decodedUrl);
        printFile.setPrintStatus(pf.getPrintStatus() != null ? pf.getPrintStatus() : "");

        printFileList.add(printFile);
    }
}
