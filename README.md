private List<SummaryProcessedFile> buildFinalProcessedList(
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> accountToArchiveMap,
        Map<String, Map<String, String>> deliveryFileMaps,
        KafkaMessage msg) {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
    Set<String> uniqueKeys = new HashSet<>();
    boolean isMfc = AppConstants.SOURCE_MFC.equalsIgnoreCase(msg.getSourceSystem());

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null || customer.getAccountNumber() == null) {
            logger.debug("[{}] ⏩ Skipping null/invalid customer entry.", msg.getBatchId());
            continue;
        }

        String account = customer.getAccountNumber();
        Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());

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

            // Safely get the folder maps (may be null if folder missing)
            Map<String, String> emailMap = deliveryFileMaps != null ? deliveryFileMaps.get(AppConstants.FOLDER_EMAIL) : null;
            Map<String, String> mobMap = deliveryFileMaps != null ? deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT) : null;
            Map<String, String> printMap = deliveryFileMaps != null ? deliveryFileMaps.get(AppConstants.FOLDER_PRINT) : null;

            if (isMfc) {
                entry.setPdfEmailFileUrl(findFileByAccount(emailMap, account));
                entry.setPdfMobstatFileUrl(findFileByAccount(mobMap, account));
                entry.setPrintFileUrl(findFileByAccount(printMap, account));
            } else {
                // try exact filename match first; fallback to account-based match
                entry.setPdfEmailFileUrl(getBestMatchUrl(emailMap, archiveFileName, account));
                entry.setPdfMobstatFileUrl(getBestMatchUrl(mobMap, archiveFileName, account));
                entry.setPrintFileUrl(getBestMatchUrl(printMap, archiveFileName, account));
            }

            finalList.add(entry);
        }
    }
    return finalList;
}

private String getBestMatchUrl(Map<String, String> fileMap, String archiveFileName, String account) {
    if (fileMap == null || fileMap.isEmpty()) return null;

    // 1) Exact filename match
    if (archiveFileName != null) {
        String url = fileMap.get(archiveFileName);
        if (url != null) return url;
    }

    // 2) Try some common filename variations
    // - match removing known suffix from archiveFileName, if any
    if (archiveFileName != null) {
        // try case-insensitive contains or endsWith
        String lowerArchive = archiveFileName.toLowerCase();
        Optional<String> match = fileMap.entrySet().stream()
                .filter(e -> {
                    String k = e.getKey() != null ? e.getKey().toLowerCase() : "";
                    return k.equals(lowerArchive)
                            || k.endsWith(lowerArchive)
                            || lowerArchive.endsWith(k)
                            || k.contains(lowerArchive)
                            || lowerArchive.contains(k);
                })
                .map(Map.Entry::getValue)
                .findFirst();
        if (match.isPresent()) return match.get();
    }

    // 3) Account based match (fallback)
    return findFileByAccount(fileMap, account);
}
private String findFileByAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null || fileMap.isEmpty() || account == null || account.isBlank()) return null;

    String acct = account.trim();
    // Normalize account for comparisons
    String acctLow = acct.toLowerCase();

    // Common patterns:
    // - account_...
    // - ..._account_...
    // - ..._account.pdf
    // - contains account anywhere
    Optional<String> opt = fileMap.entrySet().stream()
            .filter(e -> {
                String fileName = e.getKey() != null ? e.getKey().toLowerCase() : "";
                return fileName.startsWith(acctLow + "_")
                        || fileName.contains("_" + acctLow + "_")
                        || fileName.endsWith("_" + acctLow + ".pdf")
                        || fileName.equals(acctLow)
                        || fileName.contains(acctLow);
            })
            .map(Map.Entry::getValue)
            .findFirst();

    return opt.orElse(null);
}
private void processDeliveryFile(Path file, String folder,
                                 Map<String, String> folderToOutputMethod,
                                 KafkaMessage msg,
                                 Map<String, Map<String, String>> deliveryFileMaps,
                                 Map<String, Map<String, String>> errorMap) {
    if (!Files.exists(file)) {
        logger.warn("[{}] ⏩ Skipping missing {} file: {}", msg.getBatchId(), folder, file);
        return;
    }

    String fileName = file.getFileName() != null ? file.getFileName().toString() : AppConstants.UNKNOWN_FILE_NAME;
    try {
        String url = decodeUrl(
                blobStorageService.uploadFileByMessage(file.toFile(), folder, msg)
        );
        deliveryFileMaps.computeIfAbsent(folder, k -> new HashMap<>()).put(fileName, url);

        logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(),
                folderToOutputMethod.get(folder), url);
    } catch (Exception e) {
        logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(),
                folderToOutputMethod.getOrDefault(folder, folder), fileName, e.getMessage(), e);
        errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                .put(fileName, folderToOutputMethod.getOrDefault(folder, folder) + " upload failed: " + e.getMessage());
    }
}

