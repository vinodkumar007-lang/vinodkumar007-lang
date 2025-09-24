private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

    // Maps for each type per account
    Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailPdf = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailHtml = new HashMap<>();
    Map<String, Map<String, String>> accountToEmailTxt = new HashMap<>();
    Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();

    // Walk all files in jobDir
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
                        if (fileName.endsWith(".pdf")) accountToEmailPdf.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        else if (fileName.endsWith(".html")) accountToEmailHtml.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        else if (fileName.endsWith(".txt")) accountToEmailTxt.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
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

    // Build final list using full combinations
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        Map<String, String> archiveMap = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> emailPdfMap = accountToEmailPdf.getOrDefault(account, Collections.emptyMap());
        Map<String, String> emailHtmlMap = accountToEmailHtml.getOrDefault(account, Collections.emptyMap());
        Map<String, String> emailTxtMap = accountToEmailTxt.getOrDefault(account, Collections.emptyMap());
        Map<String, String> mobstatMap = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> printMap = accountToPrintFiles.getOrDefault(account, Collections.emptyMap());

        // If any map is empty, add a null placeholder to generate combinations
        List<String> archiveFiles = archiveMap.isEmpty() ? Arrays.asList((String) null) : new ArrayList<>(archiveMap.keySet());
        List<String> pdfFiles = emailPdfMap.isEmpty() ? Arrays.asList((String) null) : new ArrayList<>(emailPdfMap.values());
        List<String> htmlFiles = emailHtmlMap.isEmpty() ? Arrays.asList((String) null) : new ArrayList<>(emailHtmlMap.values());
        List<String> txtFiles = emailTxtMap.isEmpty() ? Arrays.asList((String) null) : new ArrayList<>(emailTxtMap.values());
        List<String> mobstatFiles = mobstatMap.isEmpty() ? Arrays.asList((String) null) : new ArrayList<>(mobstatMap.values());
        List<String> printFiles = printMap.isEmpty() ? Arrays.asList((String) null) : new ArrayList<>(printMap.values());

        for (String archiveEntry : archiveFiles) {
            for (String pdfUrl : pdfFiles) {
                for (String htmlUrl : htmlFiles) {
                    for (String txtUrl : txtFiles) {
                        for (String mobUrl : mobstatFiles) {
                            for (String printUrl : printFiles) {
                                SummaryProcessedFile entry = new SummaryProcessedFile();
                                BeanUtils.copyProperties(customer, entry);

                                entry.setArchiveBlobUrl(archiveEntry);
                                entry.setEmailBlobUrlPdf(pdfUrl);
                                entry.setEmailBlobUrlHtml(htmlUrl);
                                entry.setEmailBlobUrlText(txtUrl);
                                entry.setPdfMobstatFileUrl(mobUrl);
                                entry.setPrintFileUrl(printUrl);

                                finalList.add(entry);
                            }
                        }
                    }
                }
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

    for (SummaryProcessedFile file : processedFiles) {
        if (file == null) continue;

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        entry.setEmailBlobUrlPdf(file.getEmailBlobUrlPdf());
        entry.setEmailBlobUrlHtml(file.getEmailBlobUrlHtml());
        entry.setEmailBlobUrlText(file.getEmailBlobUrlText());
        entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
        entry.setArchiveBlobUrl(file.getArchiveBlobUrl());
        entry.setPrintBlobUrl(file.getPrintFileUrl());

        // Determine account
        String account = file.getAccountNumber();
        if ((account == null || account.isBlank()) && isNonEmpty(file.getArchiveBlobUrl())) {
            account = extractAccountFromFileName(new File(file.getArchiveBlobUrl()).getName());
        }

        Map<String, String> errors = errorMap.getOrDefault(account, Collections.emptyMap());

        // Individual statuses
        entry.setEmailStatus(isNonEmpty(entry.getEmailBlobUrlPdf()) || isNonEmpty(entry.getEmailBlobUrlHtml()) || isNonEmpty(entry.getEmailBlobUrlText())
                ? "SUCCESS"
                : "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
        entry.setMobstatStatus(isNonEmpty(entry.getMobstatBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
        entry.setPrintStatus(isNonEmpty(entry.getPrintBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
        entry.setArchiveStatus(isNonEmpty(entry.getArchiveBlobUrl()) ? "SUCCESS" :
                "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

        // Overall status
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

        // ErrorMap partial handling
        if (errorMap.containsKey(account) && !"FAILED".equals(entry.getOverallStatus())) {
            entry.setOverallStatus("PARTIAL");
        }

        allEntries.add(entry);
    }

    return allEntries;
}



