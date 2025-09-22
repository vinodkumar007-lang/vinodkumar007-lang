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

                // ✅ Only allow PDF and PS files
                if (!(fileName.endsWith(".pdf") || fileName.endsWith(".ps"))) {
                    logger.debug("[{}] ⏩ Skipping non-pdf/ps file: {}", msg.getBatchId(), fileName);
                    return;
                }

                try {
                    String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), parentFolder, msg));

                    // ✅ Match file to customers by account number
                    for (SummaryProcessedFile customer : customerList) {
                        if (customer == null || customer.getAccountNumber() == null) continue;
                        String account = customer.getAccountNumber();
                        if (!fileName.contains(account)) continue;

                        if (parentFolder.contains(FOLDER_ARCHIVE)) {
                            accountToArchiveFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 📦 Uploaded archive file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);
                        } else if (parentFolder.contains(FOLDER_EMAIL)) {
                            accountToEmailFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 📧 Uploaded email file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);
                        } else if (parentFolder.contains(FOLDER_MOBSTAT)) {
                            accountToMobstatFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 📱 Uploaded mobstat file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);
                        } else if (parentFolder.contains(FOLDER_PRINT)) {
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

            List<String> emailFiles = new ArrayList<>(emailsForAccount.values());
            if (emailFiles.isEmpty()) emailFiles.add(null);

            List<String> mobstatFiles = new ArrayList<>(mobstatsForAccount.values());
            if (mobstatFiles.isEmpty()) mobstatFiles.add(null);

            // ✅ Iterate over all combinations
            for (String archiveFileName : archiveFiles) {
                for (String emailUrl : emailFiles) {
                    for (String mobstatUrl : mobstatFiles) {
                        String key = customer.getCustomerId() + "|" + account + "|" +
                                (archiveFileName != null ? archiveFileName : "noArchive") + "|" +
                                (emailUrl != null ? emailUrl : "noEmail") + "|" +
                                (mobstatUrl != null ? mobstatUrl : "noMobstat");
                        if (uniqueKeys.contains(key)) continue;
                        uniqueKeys.add(key);

                        SummaryProcessedFile entry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, entry);
                        entry.setArchiveBlobUrl(archiveFileName != null ? archivesForAccount.get(archiveFileName) : null);
                        entry.setPdfEmailFileUrl(emailUrl);
                        entry.setPdfMobstatFileUrl(mobstatUrl);
                        finalList.add(entry);
                    }
                }
            }
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }


    "emailBlobUrlpdf" : "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/NGB/4f5bc8e1-ca1e-4738-821a-a9b71474dd64/18… 839420_153685.pdf",
    "emailBlobUrlhtml":  "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/NGB/4f5bc8e1-ca1e-4738-821a-a9b71474dd64/18… 839420_153685.html",
"emailBlobUrltext" : "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/NGB/4f5bc8e1-ca1e-4738-821a-a9b71474dd64/18… 839420_153685.text",
