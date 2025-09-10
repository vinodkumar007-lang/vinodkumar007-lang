    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        if (!validateInputs(jobDir, customerList, msg)) return new ArrayList<>();

        List<String> deliveryFolders = List.of(
                AppConstants.FOLDER_EMAIL,
                AppConstants.FOLDER_MOBSTAT,
                AppConstants.FOLDER_PRINT
        );

        Map<String, String> folderToOutputMethod = Map.of(
                AppConstants.FOLDER_EMAIL, AppConstants.OUTPUT_EMAIL,
                AppConstants.FOLDER_MOBSTAT, AppConstants.OUTPUT_MOBSTAT,
                AppConstants.FOLDER_PRINT, AppConstants.OUTPUT_PRINT
        );

        // Step 1: Upload archive files
        Map<String, Map<String, String>> accountToArchiveMap = uploadArchiveFiles(jobDir, msg, errorMap);

        // Step 2: Upload delivery files
        Map<String, Map<String, String>> deliveryFileMaps = uploadDeliveryFiles(jobDir, deliveryFolders, folderToOutputMethod, msg, errorMap);

        // Step 3: Build final processed list (one entry per file)
        List<SummaryProcessedFile> finalList = buildFinalProcessedList(customerList, accountToArchiveMap, deliveryFileMaps, msg);

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Total entries={}", msg.getBatchId(), finalList.size());
        return finalList;
    }

    private boolean validateInputs(Path jobDir, List<SummaryProcessedFile> customerList, KafkaMessage msg) {
        if (jobDir == null) {
            logger.warn("[{}] ⚠️ jobDir is null. Skipping processing.", msg != null ? msg.getBatchId() : "UNKNOWN");
            return false;
        }
        if (customerList == null || customerList.isEmpty()) {
            logger.warn("[{}] ⚠️ customerList is empty. Nothing to process.", msg != null ? msg.getBatchId() : "UNKNOWN");
            return false;
        }
        if (msg == null) {
            logger.warn("[UNKNOWN] ⚠️ KafkaMessage is null. Skipping processing.");
            return false;
        }
        return true;
    }

    private Map<String, Map<String, String>> uploadArchiveFiles(Path jobDir, KafkaMessage msg, Map<String, Map<String, String>> errorMap) throws IOException {
        Map<String, Map<String, String>> accountToArchiveMap = new HashMap<>();
        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);

        if (!Files.exists(archivePath)) return accountToArchiveMap;

        try (Stream<Path> stream = Files.walk(archivePath)) {
            stream.filter(Files::isRegularFile)
                    .filter(f -> !f.getFileName().toString().endsWith(".tmp"))
                    .forEach(f -> processFile(f, AppConstants.FOLDER_ARCHIVE, msg, accountToArchiveMap, errorMap));
        }

        return accountToArchiveMap;
    }

    private Map<String, Map<String, String>> uploadDeliveryFiles(Path jobDir,
                                                                 List<String> deliveryFolders,
                                                                 Map<String, String> folderToOutputMethod,
                                                                 KafkaMessage msg,
                                                                 Map<String, Map<String, String>> errorMap) throws IOException {
        Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();
        for (String folder : deliveryFolders) deliveryFileMaps.put(folder, new HashMap<>());

        try (Stream<Path> allDirs = Files.walk(jobDir)) {
            List<Path> folderPaths = allDirs
                    .filter(Files::isDirectory)
                    .filter(p -> deliveryFolders.stream().anyMatch(f -> f.equalsIgnoreCase(p.getFileName().toString())))
                    .toList();

            for (Path folderPath : folderPaths) {
                String folderName = folderPath.getFileName().toString();
                try (Stream<Path> files = Files.walk(folderPath)) {
                    files.filter(Files::isRegularFile)
                            .filter(f -> !f.getFileName().toString().endsWith(".tmp"))
                            .forEach(f -> processFile(f, folderName, msg, deliveryFileMaps, errorMap));
                }
            }
        }

        return deliveryFileMaps;
    }

    private void processFile(Path file, String folder, KafkaMessage msg,
                             Map<String, Map<String, String>> fileMap,
                             Map<String, Map<String, String>> errorMap) {
        if (!Files.exists(file)) return;

        String fileName = file.getFileName().toString();
        try {
            String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), folder, msg));
            fileMap.computeIfAbsent(folder, k -> new HashMap<>()).put(fileName, url);
            logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folder, url);
        } catch (Exception e) {
            errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                    .put(fileName, folder + " upload failed: " + e.getMessage());
        }
    }

    private List<SummaryProcessedFile> buildFinalProcessedList(List<SummaryProcessedFile> customerList,
                                                               Map<String, Map<String, String>> accountToArchiveMap,
                                                               Map<String, Map<String, String>> deliveryFileMaps,
                                                               KafkaMessage msg) {

        List<SummaryProcessedFile> finalList = new ArrayList<>();

        for (SummaryProcessedFile customer : customerList) {
            if (customer == null || customer.getAccountNumber() == null) continue;
            String account = customer.getAccountNumber();

            Map<String, String> archives = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());
            Map<String, String> emails = deliveryFileMaps.getOrDefault(AppConstants.FOLDER_EMAIL, Collections.emptyMap());
            Map<String, String> mobstats = deliveryFileMaps.getOrDefault(AppConstants.FOLDER_MOBSTAT, Collections.emptyMap());
            Map<String, String> prints = deliveryFileMaps.getOrDefault(AppConstants.FOLDER_PRINT, Collections.emptyMap());

            // Archive entries
            for (Map.Entry<String, String> entry : archives.entrySet()) {
                if (!entry.getKey().contains(account)) continue;
                SummaryProcessedFile spf = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, spf);
                spf.setArchiveBlobUrl(entry.getValue());
                finalList.add(spf);
            }

            // Email entries
            for (Map.Entry<String, String> entry : emails.entrySet()) {
                if (!entry.getKey().contains(account)) continue;
                SummaryProcessedFile spf = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, spf);
                spf.setPdfEmailFileUrl(entry.getValue());
                finalList.add(spf);
            }

            // Mobstat entries
            for (Map.Entry<String, String> entry : mobstats.entrySet()) {
                if (!entry.getKey().contains(account)) continue;
                SummaryProcessedFile spf = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, spf);
                spf.setPdfMobstatFileUrl(entry.getValue());
                finalList.add(spf);
            }

            // Print entries
            for (Map.Entry<String, String> entry : prints.entrySet()) {
                if (!entry.getKey().contains(account)) continue;
                SummaryProcessedFile spf = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, spf);
                spf.setPrintFileUrl(entry.getValue());
                finalList.add(spf);
            }
        }

        logger.info("[{}] ✅ buildFinalProcessedList completed. Total entries={}", msg.getBatchId(), finalList.size());
        return finalList;
    }

    private String extractAccountFromFileName(String fileName) {
        if (fileName == null) return null;
        Matcher matcher = Pattern.compile("\\d{10,12}").matcher(fileName);
        if (matcher.find()) return matcher.group();
        return null;
    }
