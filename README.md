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

        // -------- 1️⃣ Upload all archive files once and map by account --------
        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
        Map<String, String> accountToArchiveUrl = new HashMap<>();
        if (Files.exists(archivePath)) {
            try (Stream<Path> stream = Files.walk(archivePath)) {
                List<Path> archiveFiles = stream.filter(Files::isRegularFile).toList();

                for (Path archiveFile : archiveFiles) {
                    String fileName = archiveFile.getFileName().toString();
                    String account = extractAccountFromFileName(fileName); // implement method to extract account
                    if (account == null) continue;

                    try {
                        String archiveBlobUrl = blobStorageService.uploadFileByMessage(
                                archiveFile.toFile(), AppConstants.FOLDER_ARCHIVE, msg);
                        archiveBlobUrl = decodeUrl(archiveBlobUrl);

                        accountToArchiveUrl.put(account, archiveBlobUrl);
                        logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveBlobUrl);

                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed scanning ARCHIVE folder: {}", msg.getBatchId(), e.getMessage(), e);
            }
        }

        // -------- 2️⃣ Process each customer --------
        for (SummaryProcessedFile customer : customerList) {
            if (customer == null) continue;
            String account = customer.getAccountNumber();
            if (account == null || account.isBlank()) continue;

            // Add archive entry for this customer/account
            if (accountToArchiveUrl.containsKey(account)) {
                SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, archiveEntry);
                archiveEntry.setOutputType(AppConstants.OUTPUT_ARCHIVE);
                archiveEntry.setFileName(account + "_ARCHIVE.pdf"); // optional, or real file name if stored
                archiveEntry.setBlobUrl(accountToArchiveUrl.get(account));
                archiveEntry.setArchiveStatus("SUCCESS");
                archiveEntry.setOverallStatus("SUCCESS");
                finalList.add(archiveEntry);
            }

            // -------- 3️⃣ Upload delivery files and map archive --------
            for (String folder : deliveryFolders) {
                Path methodPath = jobDir.resolve(folder);
                if (!Files.exists(methodPath)) continue;

                String outputMethod = folderToOutputMethod.get(folder);
                try (Stream<Path> stream = Files.walk(methodPath)) {
                    List<Path> deliveryFiles = stream
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .toList();

                    for (Path deliveryFile : deliveryFiles) {
                        try {
                            String deliveryBlobUrl = blobStorageService.uploadFileByMessage(deliveryFile.toFile(), folder, msg);
                            deliveryBlobUrl = decodeUrl(deliveryBlobUrl);

                            SummaryProcessedFile entry = new SummaryProcessedFile();
                            BeanUtils.copyProperties(customer, entry);
                            entry.setOutputType(outputMethod);
                            entry.setFileName(deliveryFile.getFileName().toString());

                            // Attach delivery URL
                            switch (folder) {
                                case AppConstants.FOLDER_EMAIL -> entry.setPdfEmailFileUrl(deliveryBlobUrl);
                                case AppConstants.FOLDER_MOBSTAT -> entry.setPdfMobstatFileUrl(deliveryBlobUrl);
                                case AppConstants.FOLDER_PRINT -> entry.setPrintFileUrl(deliveryBlobUrl);
                            }

                            // Attach archive URL for this account
                            entry.setArchiveBlobUrl(accountToArchiveUrl.get(account));

                            entry.setOverallStatus("SUCCESS");
                            finalList.add(entry);

                            logger.info("[{}] ✅ Uploaded {} file for account {}: {}", msg.getBatchId(), outputMethod, account, deliveryBlobUrl);

                        } catch (Exception e) {
                            logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}", msg.getBatchId(), outputMethod, account, e.getMessage(), e);
                        }
                    }
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to scan folder {} for account {}: {}", msg.getBatchId(), folder, account, e.getMessage(), e);
                }
            }
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }

private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> printFiles) {

        List<ProcessedFileEntry> allEntries = new ArrayList<>();

        for (SummaryProcessedFile file : processedFiles) {
            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            String outputType = file.getOutputType();
            String blobUrl = file.getBlobUrl();
            Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

            // Determine delivery status
            String status;
            if (isNonEmpty(blobUrl)) {
                status = "SUCCESS";
            } else if ("FAILED".equalsIgnoreCase(errors.getOrDefault(outputType, ""))) {
                status = "FAILED";
            } else {
                status = "";
            }

            switch (outputType) {
                case "EMAIL" -> {
                    entry.setEmailBlobUrl(blobUrl);
                    entry.setEmailStatus(status);
                }
                case "PRINT" -> {
                    if (printFiles != null && !printFiles.isEmpty()) {
                        entry.setPrintStatus("SUCCESS");
                    } else {
                        entry.setPrintStatus("");
                    }
                }
                case "MOBSTAT" -> {
                    entry.setMobstatBlobUrl(blobUrl);
                    entry.setMobstatStatus(status);
                }
                case "ARCHIVE" -> {
                    entry.setArchiveBlobUrl(blobUrl);
                    entry.setArchiveStatus(status);
                }
            }

            // ---- overallStatus per entry (each archive combination gets its own record) ----
            String email = entry.getEmailStatus();
            String print = entry.getPrintStatus();
            String mobstat = entry.getMobstatStatus();
            String archive = entry.getArchiveStatus();

            boolean isEmailSuccess = "SUCCESS".equals(email);
            boolean isPrintSuccess = "SUCCESS".equals(print);
            boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
            boolean isArchiveSuccess = "SUCCESS".equals(archive);

            boolean isEmailMissingOrFailed = email == null || "FAILED".equals(email) || "".equals(email);
            boolean isPrintMissingOrFailed = print == null || "FAILED".equals(print) || "".equals(print);
            boolean isMobstatMissingOrFailed = mobstat == null || "FAILED".equals(mobstat) || "".equals(mobstat);

            if (isEmailSuccess && isArchiveSuccess) {
                entry.setOverallStatus("SUCCESS");
            } else if (isMobstatSuccess && isArchiveSuccess && isEmailMissingOrFailed && isPrintMissingOrFailed) {
                entry.setOverallStatus("SUCCESS");
            } else if (isPrintSuccess && isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed) {
                entry.setOverallStatus("SUCCESS");
            } else if (isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed && isPrintMissingOrFailed) {
                entry.setOverallStatus("PARTIAL");
            } else if (isArchiveSuccess) {
                entry.setOverallStatus("PARTIAL");
            } else {
                entry.setOverallStatus("FAILED");
            }

            if (errorMap.containsKey(entry.getAccountNumber())
                    && !"FAILED".equals(entry.getOverallStatus())) {
                entry.setOverallStatus("PARTIAL");
            }

            allEntries.add(entry);
        }

        return allEntries;
    }
