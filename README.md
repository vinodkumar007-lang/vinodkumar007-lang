private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

    Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailPdf = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailHtml = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailTxt = new HashMap<>();
    Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();

    try (Stream<Path> stream = Files.walk(jobDir)) {
        stream.filter(Files::isRegularFile).forEach(file -> {
            if (!Files.exists(file)) return;

            String fileName = file.getFileName().toString().toLowerCase();
            String parentFolder = file.getParent().getFileName().toString().toLowerCase();

            try {
                String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), parentFolder, msg));

                for (SummaryProcessedFile customer : customerList) {
                    if (customer == null || customer.getAccountNumber() == null) continue;
                    String account = customer.getAccountNumber();
                    if (!fileName.contains(account)) continue;

                    if (parentFolder.contains("archive") && fileName.endsWith(".pdf")) {
                        accountToArchiveFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains("email")) {
                        if (fileName.endsWith(".pdf")) {
                            accountToEmailPdf.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        } else if (fileName.endsWith(".html")) {
                            accountToEmailHtml.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        } else if (fileName.endsWith(".txt")) {
                            accountToEmailTxt.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        }
                    } else if (parentFolder.contains("mobstat") && fileName.endsWith(".pdf")) {
                        accountToMobstatFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    } else if (parentFolder.contains("print") && fileName.endsWith(".ps")) {
                        accountToPrintFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                    }
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to upload file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
            }
        });
    }

    // Build final list (no duplicate skipping except identical full record)
    Set<String> uniqueKeys = new HashSet<>();
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        Map<String, String> archiveMap = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> emailPdfMap = accountToEmailPdf.getOrDefault(account, Collections.emptyMap());
        Map<String, String> emailHtmlMap = accountToEmailHtml.getOrDefault(account, Collections.emptyMap());
        Map<String, String> emailTxtMap = accountToEmailTxt.getOrDefault(account, Collections.emptyMap());
        Map<String, String> mobstatMap = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> printMap = accountToPrintFiles.getOrDefault(account, Collections.emptyMap());

        List<String> archiveFiles = new ArrayList<>(archiveMap.keySet());
        if (archiveFiles.isEmpty()) archiveFiles.add(null);

        for (String archiveFileName : archiveFiles) {
            String pdfUrl = emailPdfMap.values().stream().findFirst().orElse(null);
            String htmlUrl = emailHtmlMap.values().stream().findFirst().orElse(null);
            String txtUrl = emailTxtMap.values().stream().findFirst().orElse(null);
            String mobstatUrl = mobstatMap.values().stream().findFirst().orElse(null);
            String printUrl = printMap.values().stream().findFirst().orElse(null);

            String uniqueKey = customer.getCustomerId() + "|" + account + "|" +
                    (archiveFileName != null ? archiveFileName : "noArchive") + "|" +
                    pdfUrl + "|" + htmlUrl + "|" + txtUrl + "|" + mobstatUrl + "|" + printUrl;

            if (uniqueKeys.contains(uniqueKey)) continue;
            uniqueKeys.add(uniqueKey);

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);

            entry.setArchiveBlobUrl(archiveFileName != null ? archiveMap.get(archiveFileName) : null);
            entry.setEmailBlobUrlPdf(pdfUrl);
            entry.setEmailBlobUrlHtml(htmlUrl);
            entry.setEmailBlobUrlText(txtUrl);
            entry.setPdfMobstatFileUrl(mobstatUrl);
            entry.setPrintFileUrl(printUrl);

            finalList.add(entry);
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

        // Unique key now includes all file URLs
        String key = file.getCustomerId() + "|" + file.getAccountNumber() + "|" +
                (file.getArchiveBlobUrl() != null ? new File(file.getArchiveBlobUrl()).getName() : "noArchive") + "|" +
                file.getEmailBlobUrlPdf() + "|" + file.getEmailBlobUrlHtml() + "|" +
                file.getEmailBlobUrlText() + "|" + file.getPdfMobstatFileUrl() + "|" + file.getPrintFileUrl();

        if (uniqueKeys.contains(key)) continue;
        uniqueKeys.add(key);

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());
        entry.setEmailBlobUrlPdf(file.getEmailBlobUrlPdf());
        entry.setEmailBlobUrlHtml(file.getEmailBlobUrlHtml());
        entry.setEmailBlobUrlText(file.getEmailBlobUrlText());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());

        // --- Status logic stays same ---
        String account = file.getAccountNumber();
        if ((account == null || account.isBlank()) && isNonEmpty(file.getArchiveBlobUrl())) {
            account = extractAccountFromFileName(new File(file.getArchiveBlobUrl()).getName());
        }

        Map<String, String> errors = errorMap.getOrDefault(account, Collections.emptyMap());

        entry.setEmailStatus(
                isNonEmpty(entry.getEmailBlobUrlPdf()) || isNonEmpty(entry.getEmailBlobUrlHtml()) || isNonEmpty(entry.getEmailBlobUrlText())
                        ? "SUCCESS"
                        : "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : ""
        );
        entry.setMobstatStatus(isNonEmpty(entry.getMobstatBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(entry.getPrintBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(entry.getArchiveBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

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
