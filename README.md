MFC---Statement-2025-07-23_29104120001.pdf
 
Debtman--1102504092_LTRCR004.pdf




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
            logger.info("[{}] ➡️ Processing customer with accountNumber={}", msg.getBatchId(), account);

            if (account == null || account.isBlank()) {
                logger.warn("[{}] ⚠️ Skipping customer with empty account number", msg.getBatchId());
                continue;
            }

            // -------- ARCHIVE upload --------
            String archiveBlobUrl = null;
            try {
                if (Files.exists(archivePath)) {
                    logger.debug("[{}] 📄 Scanning archive folder recursively for account {}", msg.getBatchId(), account);

                    try (Stream<Path> stream = Files.walk(archivePath)) {
                        Optional<Path> archiveFile = stream
                                .filter(Files::isRegularFile)
                                .filter(p -> {
                                    boolean fileNameMatch = p.getFileName().toString().contains(account);
                                    logger.trace("[{}]   Checking archive file={} -> match={}",
                                            msg.getBatchId(), p.getFileName(), fileNameMatch);
                                    return fileNameMatch;
                                })
                                .findFirst();

                        if (archiveFile.isPresent()) {
                            archiveBlobUrl = blobStorageService.uploadFileByMessage(
                                    archiveFile.get().toFile(), AppConstants.FOLDER_ARCHIVE, msg);

                            SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                            BeanUtils.copyProperties(customer, archiveEntry);
                            archiveEntry.setOutputType(AppConstants.OUTPUT_ARCHIVE);
                            archiveEntry.setBlobUrl(decodeUrl(archiveBlobUrl));

                            finalList.add(archiveEntry);
                            logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveBlobUrl);
                        } else {
                            logger.warn("[{}] ❌ No archive file found for account {}", msg.getBatchId(), account);
                        }
                    }
                } else {
                    logger.warn("[{}] ❌ Archive folder does not exist: {}", msg.getBatchId(), archivePath);
                }
            } catch (Exception e) {
                logger.error("[{}] ⚠️ Failed to upload archive file for account {}: {}", msg.getBatchId(), account, e.getMessage(), e);
            }

            // -------- EMAIL, MOBSTAT, PRINT uploads --------
            for (String folder : deliveryFolders) {
                String outputMethod = folderToOutputMethod.get(folder);
                Path methodPath = jobDir.resolve(folder);
                String blobUrl = null;

                logger.info("[{}] 📂 Checking folder='{}' for account {} at path {}", msg.getBatchId(), folder, account, methodPath);

                try {
                    if (Files.exists(methodPath)) {
                        try (Stream<Path> stream = Files.walk(methodPath)) {
                            Optional<Path> matchedFile = stream
                                    .filter(Files::isRegularFile)
                                    .filter(p -> {
                                        boolean fileNameMatch = p.getFileName().toString().contains(account);
                                        logger.trace("[{}]   Checking {} file={} -> match={}",
                                                msg.getBatchId(), folder, p.getFileName(), fileNameMatch);
                                        return fileNameMatch;
                                    })
                                    .findFirst();

                            if (matchedFile.isPresent()) {
                                blobUrl = blobStorageService.uploadFileByMessage(matchedFile.get().toFile(), folder, msg);
                                logger.info("[{}] ✅ Uploaded {} file for account {}: {}", msg.getBatchId(), outputMethod, account, blobUrl);
                            } else {
                                logger.warn("[{}] ❌ No matching file found in {} for account {}", msg.getBatchId(), folder, account);
                            }
                        }
                    } else {
                        logger.warn("[{}] ❌ Folder '{}' does not exist at path {}", msg.getBatchId(), folder, methodPath);
                    }
                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload {} file for account {}: {}", msg.getBatchId(), outputMethod, account, e.getMessage(), e);
                }

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputType(outputMethod);
                entry.setBlobUrl(decodeUrl(blobUrl));

                if (archiveBlobUrl != null) {
                    entry.setArchiveOutputType(AppConstants.OUTPUT_ARCHIVE);
                    entry.setArchiveBlobUrl(archiveBlobUrl);
                }

                finalList.add(entry);
            }
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }


Checking folder='mobstat' for account 29099420001 at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/mobstat
2025-08-18T11:31:03.829+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Folder 'mobstat' does not exist at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/mobstat
d7dfb9-cb41-4a47-8438-8e686b0aec52] 📂 Checking folder='print' for account 29099420001 at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/print
2025-08-18T11:31:03.829+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Folder 'print' does not exist at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/print
2025-08-18T11:31:03.829+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ➡️ Processing customer with accountNumber=29104120001
d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Archive folder does not exist: /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/archive
2025-08-18T11:31:03.829+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📂 Checking folder='email' for account 29104120001 at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/email
2025-08-18T11:31:03.829+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Folder 'email' does not exist at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/email
2025-08-18T11:31:03.829+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📂 Checking folder='mobstat' for account 29104120001 at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/mobstat
2025-08-18T11:31:03.829+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Folder 'mobstat' does not exist at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/mobstat
2025-08-18T11:31:03.829+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📂 Checking folder='print' for account 29104120001 at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/print
2025-08-18T11:31:03.829+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Folder 'print' does not exist at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/6524f221-b128-4039-8cac-821f3f803abb/print
2025-08-18T11:31:03.829+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ➡️ Processing customer with accountNumber=29110140001
