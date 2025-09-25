private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    if (jobDir == null || customerList == null || msg == null) return finalList;

    // Maps for each type
    Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
    Map<String, Map<String, List<String>>> accountToEmailFiles = new HashMap<>(); // updated to List<String>
    Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
    Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();

    // Walk ALL folders inside jobDir
    try (Stream<Path> stream = Files.walk(jobDir)) {
        stream.filter(Files::isRegularFile).forEach(file -> {
            if (!Files.exists(file)) {
                logger.warn("[{}] ⏩ Skipping missing file: {}", msg.getBatchId(), file);
                return;
            }

            String fileName = file.getFileName().toString().toLowerCase();
            String parentFolder = file.getParent().getFileName().toString().toLowerCase();

            // Only allow PDF, PS, HTML, TXT
            if (!(fileName.endsWith(".pdf") || fileName.endsWith(".ps") ||
                  fileName.endsWith(".html") || fileName.endsWith(".txt"))) {
                logger.debug("[{}] ⏩ Skipping unsupported file: {}", msg.getBatchId(), fileName);
                return;
            }

            try {
                String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), parentFolder, msg));

                // Match file to customers by account number
                for (SummaryProcessedFile customer : customerList) {
                    if (customer == null || customer.getAccountNumber() == null) continue;
                    String account = customer.getAccountNumber();
                    if (!fileName.contains(account)) continue;

                    if (parentFolder.contains("archive")) {
                        accountToArchiveFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                        logger.info("[{}] 📦 Uploaded archive file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);

                    } else if (parentFolder.contains("email")) {
                        Map<String, List<String>> fileMap = accountToEmailFiles.computeIfAbsent(account, k -> new HashMap<>());
                        if (fileName.endsWith(".pdf")) {
                            fileMap.computeIfAbsent("PDF", k -> new ArrayList<>()).add(url);
                        } else if (fileName.endsWith(".html")) {
                            fileMap.computeIfAbsent("HTML", k -> new ArrayList<>()).add(url);
                        } else if (fileName.endsWith(".txt")) {
                            fileMap.computeIfAbsent("TEXT", k -> new ArrayList<>()).add(url);
                        } else {
                            fileMap.computeIfAbsent(fileName, k -> new ArrayList<>()).add(url);
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

    // Build final list with fixed combinations
    Set<String> uniqueKeys = new HashSet<>();
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) continue;
        String account = customer.getAccountNumber();

        Map<String, String> archivesForAccount = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, List<String>> emailsForAccount = accountToEmailFiles.getOrDefault(account, Collections.emptyMap());
        Map<String, String> mobstatsForAccount = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());

        // Skip if nothing exists
        if (archivesForAccount.isEmpty() && emailsForAccount.isEmpty() && mobstatsForAccount.isEmpty()) continue;

        List<String> archiveFiles = new ArrayList<>(archivesForAccount.keySet());
        if (archiveFiles.isEmpty()) archiveFiles.add(null);

        List<String> mobstatFiles = new ArrayList<>(mobstatsForAccount.values());
        if (mobstatFiles.isEmpty()) mobstatFiles.add(null);

        List<String> pdfEmails = emailsForAccount.getOrDefault("PDF", List.of((String) null));
        List<String> htmlEmails = emailsForAccount.getOrDefault("HTML", List.of((String) null));
        List<String> txtEmails = emailsForAccount.getOrDefault("TEXT", List.of((String) null));

        for (String archiveFileName : archiveFiles) {
            for (String mobstatUrl : mobstatFiles) {
                for (String pdfEmail : pdfEmails) {
                    for (String htmlEmail : htmlEmails) {
                        for (String txtEmail : txtEmails) {

                            String key = customer.getCustomerId() + "|" + account + "|" +
                                    (archiveFileName != null ? archiveFileName : "noArchive") + "|" +
                                    (mobstatUrl != null ? mobstatUrl : "noMobstat") + "|" +
                                    (pdfEmail != null ? pdfEmail : "noPdf") + "|" +
                                    (htmlEmail != null ? htmlEmail : "noHtml") + "|" +
                                    (txtEmail != null ? txtEmail : "noTxt");

                            if (uniqueKeys.contains(key)) continue;
                            uniqueKeys.add(key);

                            SummaryProcessedFile entry = new SummaryProcessedFile();
                            BeanUtils.copyProperties(customer, entry);

                            entry.setArchiveBlobUrl(archiveFileName != null ? archivesForAccount.get(archiveFileName) : null);
                            entry.setPdfMobstatFileUrl(mobstatUrl);
                            entry.setEmailBlobUrlPdf(pdfEmail);
                            entry.setEmailBlobUrlHtml(htmlEmail);
                            entry.setEmailBlobUrlText(txtEmail);

                            finalList.add(entry);
                        }
                    }
                }
            }
        }
    }

    logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
    return finalList;
}


2025-09-25T11:30:55.659+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsndevextrm01/DEBTMAN%2Fa4aff1c2-f726-481b-bcff-41f4851d9c59%2F19ef9d68-b114-4803-b09b-95a6c5fa4644%2Fprint%2FPRODDebtmanNormal_HL_20250906.ps'
2025-09-25T11:30:55.814+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsndevextrm01/DEBTMAN%2Fa4aff1c2-f726-481b-bcff-41f4851d9c59%2F19ef9d68-b114-4803-b09b-95a6c5fa4644%2Fprint%2FPRODDebtmanRegistered_RB_20250906.ps'
2025-09-25T11:30:55.815+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : [a4aff1c2-f726-481b-bcff-41f4851d9c59] ❌ Error post-OT summary generation: null
java.lang.NullPointerException: null
 at java.base/java.util.Objects.requireNonNull(Objects.java:209) ~[na:na]
 at java.base/java.util.ImmutableCollections$List12.<init>(ImmutableCollections.java:556) ~[na:na]
 at java.base/java.util.List.of(List.java:812) ~[na:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.buildDetailedProcessedFiles(KafkaListenerService.java:796) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:362) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$2(KafkaListenerService.java:223) ~[classes!/:na]
 at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
 at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
 at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
 at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
 at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]
2025-09-25T11:30:55.862+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Resetting the last seen epoch of partition log-ecp-batch-audit-0 to 0 since the associated topicId changed from null to LQT4uTbBRtSuzkoLLTIsJA
2025-09-25T11:30:55.862+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Resetting the last seen epoch of partition log-ecp-batch-audit-1 to 0 since the associated topicId changed from null to LQT4uTbBRtSuzkoLLTIsJA
2025-09-25T11:30:55.862+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Resetting the last seen epoch of partition log-ecp-batch-audit-2 to 0 since the associated topicId changed from null to LQT4uTbBRtSuzkoLLTIsJA
