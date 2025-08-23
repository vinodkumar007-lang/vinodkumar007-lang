private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
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

    logger.info("[{}] 🔍 Entered buildDetailedProcessedFiles with jobDir={}, customerList size={}",
            msg.getBatchId(), jobDir, (customerList != null ? customerList.size() : null));

    if (jobDir == null || customerList == null || msg == null) {
        logger.warn("[{}] ⚠️ One or more input parameters are null: jobDir={}, customerList={}, msg={}",
                (msg != null ? msg.getBatchId() : "N/A"), jobDir, customerList, msg);
        return finalList;
    }

    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    logger.debug("[{}] 📂 Archive folder path resolved to: {}", msg.getBatchId(), archivePath);

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) {
            logger.debug("[{}] ⚠️ Skipping null customer entry", msg.getBatchId());
            continue;
        }

        String account = customer.getAccountNumber();
        String cis = customer.getCustomerId();
        if (account == null || account.isBlank()) {
            logger.warn("[{}] ⚠️ Skipping customer with empty account number", msg.getBatchId());
            continue;
        }

        logger.info("[{}] ➡️ Processing customer accountNumber={}, cis={}", msg.getBatchId(), account, cis);

        final java.util.concurrent.atomic.AtomicReference<String> archiveBlobUrlRef = new java.util.concurrent.atomic.AtomicReference<>(null);

        // -------- ARCHIVE upload (AccountNumber OR CISNumber match) --------
        if (Files.exists(archivePath)) {
            try (Stream<Path> stream = Files.walk(archivePath)) {
                Optional<Path> archiveFile = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> fileNameMatches(p.getFileName().toString(), account, cis))  // ✅ FIX
                        .findFirst();

                if (archiveFile.isPresent()) {
                    try {
                        String archiveBlobUrl = blobStorageService.uploadFileByMessage(
                                archiveFile.get().toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                        archiveBlobUrlRef.set(decodeUrl(archiveBlobUrl));

                        SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, archiveEntry);
                        archiveEntry.setOutputType(AppConstants.OUTPUT_ARCHIVE);
                        archiveEntry.setBlobUrl(archiveBlobUrlRef.get());
                        finalList.add(archiveEntry);

                        logger.info("[{}] 📦 Uploaded archive for {}: {}", msg.getBatchId(), account, archiveBlobUrl);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload archive for {}: {}", msg.getBatchId(), account, e.getMessage(), e);
                    }
                } else {
                    logger.debug("[{}] No archive match by account/cis for account {}", msg.getBatchId(), account);
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed scanning ARCHIVE folder for {}: {}", msg.getBatchId(), account, e.getMessage(), e);
            }
        }

        // -------- EMAIL, MOBSTAT, PRINT uploads --------
        for (String folder : deliveryFolders) {
            Path methodPath = jobDir.resolve(folder);

            if (!Files.exists(methodPath)) {
                logger.debug("[{}] Folder '{}' does not exist at path {}. Skipping.", msg.getBatchId(), folder, methodPath);
                continue;
            }

            String outputMethod = folderToOutputMethod.get(folder);

            try (Stream<Path> stream = Files.walk(methodPath)) {
                stream.filter(Files::isRegularFile)
                        .filter(p -> fileNameMatches(p.getFileName().toString(), account, cis))  // ✅ FIX
                        .forEach(p -> {
                            try {
                                String blobUrl = blobStorageService.uploadFileByMessage(p.toFile(), folder, msg);
                                logger.info("[{}] ✅ Uploaded {} file for {}: {}", msg.getBatchId(), outputMethod, account, blobUrl);

                                SummaryProcessedFile entry = new SummaryProcessedFile();
                                BeanUtils.copyProperties(customer, entry);
                                entry.setOutputType(outputMethod);
                                entry.setBlobUrl(decodeUrl(blobUrl));

                                if (archiveBlobUrlRef.get() != null) {
                                    entry.setArchiveOutputType(AppConstants.OUTPUT_ARCHIVE);
                                    entry.setArchiveBlobUrl(archiveBlobUrlRef.get());
                                }

                                finalList.add(entry);
                            } catch (Exception e) {
                                logger.error("[{}] ⚠️ Failed to upload {} for {}: {}", msg.getBatchId(), outputMethod, account, e.getMessage(), e);
                            }
                        });
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to scan folder {} for {}: {}", msg.getBatchId(), folder, account, e.getMessage(), e);
            }
        }
    }

    // quick telemetry so QA can verify counts easily in logs
    long archiveCnt = finalList.stream().filter(f -> AppConstants.OUTPUT_ARCHIVE.equalsIgnoreCase(f.getOutputType())).count();
    long emailCnt   = finalList.stream().filter(f -> AppConstants.OUTPUT_EMAIL.equalsIgnoreCase(f.getOutputType())).count();
    long mobCnt     = finalList.stream().filter(f -> AppConstants.OUTPUT_MOBSTAT.equalsIgnoreCase(f.getOutputType())).count();
    long printCnt   = finalList.stream().filter(f -> AppConstants.OUTPUT_PRINT.equalsIgnoreCase(f.getOutputType())).count();
    logger.info("[{}] ✅ buildDetailedProcessedFiles done. Archive={}, Email={}, Mobstat={}, Print={}, Total={}",
            msg.getBatchId(), archiveCnt, emailCnt, mobCnt, printCnt, finalList.size());

    return finalList;
}

/** filename match helper: true if name contains account or cis (null-safe) */
private boolean fileNameMatches(String name, String account, String cis) {
    if (name == null) return false;
    boolean byAcc = (account != null && !account.isBlank() && name.contains(account));
    boolean byCis = (cis != null && !cis.isBlank() && name.contains(cis));
    return byAcc || byCis;
}
