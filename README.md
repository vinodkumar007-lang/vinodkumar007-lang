private List<SummaryProcessedFile> buildFinalProcessedList(
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> accountToArchiveMap,
        Map<String, Map<String, String>> deliveryFileMaps,
        KafkaMessage msg) {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    Set<String> uniqueKeys = new HashSet<>();
    boolean isMfc = AppConstants.SOURCE_MFC.equalsIgnoreCase(msg.getSourceSystem());

    // Step 1: Build account -> delivery map for easy lookup
    Map<String, Map<String, String>> accountToDeliveryMap = new HashMap<>();
    Map<String, List<String>> accountToPrintFiles = new HashMap<>(); // NEW: list of .ps files

    for (String folder : deliveryFileMaps.keySet()) {
        Map<String, String> files = deliveryFileMaps.get(folder);
        for (Map.Entry<String, String> entry : files.entrySet()) {
            String fileName = entry.getKey();
            String url = entry.getValue();
            String account = extractAccountFromFileName(fileName);
            if (account == null) account = "UNKNOWN";

            if (AppConstants.FOLDER_PRINT.equals(folder)) {
                accountToPrintFiles.computeIfAbsent(account, k -> new ArrayList<>()).add(url);
            } else {
                accountToDeliveryMap.computeIfAbsent(account, k -> new HashMap<>()).put(folder, url);
            }
        }
    }

    // Step 2: Build final processed list
    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) {
            logger.debug("[{}] ⏩ Skipping null/invalid customer entry.", msg.getBatchId());
            continue;
        }

        String account = customer.getAccountNumber();
        Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());
        Map<String, String> deliveryForAccount = accountToDeliveryMap.getOrDefault(account, Collections.emptyMap());
        List<String> printUrls = accountToPrintFiles.getOrDefault(account, Collections.emptyList());

        for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
            String archiveFileName = archiveEntry.getKey();
            String archiveUrl = archiveEntry.getValue();

            String key = customer.getCustomerId() + "|" + account + "|" + archiveFileName;
            if (!uniqueKeys.add(key)) {
                logger.debug("[{}] ⏩ Duplicate entry skipped for key={}", msg.getBatchId(), key);
                continue;
            }

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setArchiveBlobUrl(archiveUrl);

            // Step 2a: Assign delivery URLs
            if (isMfc) {
                entry.setPdfEmailFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL), account));
                entry.setPdfMobstatFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT), account));
            } else {
                entry.setPdfEmailFileUrl(deliveryForAccount.get(AppConstants.FOLDER_EMAIL));
                entry.setPdfMobstatFileUrl(deliveryForAccount.get(AppConstants.FOLDER_MOBSTAT));
            }

            // NEW: assign list of print files
            entry.setPrintFileUrls(printUrls);

            finalList.add(entry);
        }
    }

    logger.info("[{}] ✅ buildFinalProcessedList completed. Total entries={}", msg.getBatchId(), finalList.size());
    return finalList;
}
