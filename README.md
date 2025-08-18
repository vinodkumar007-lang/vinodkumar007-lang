
2025-08-18T06:54:00.344+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Folder 'print' does not exist at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/debbd290-4ac5-4282-9868-606ffa4caab9/print
2025-08-18T06:54:00.424+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ➡️ Processing customer with accountNumber=27713040001
2025-08-18T06:54:00.424+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Archive folder does not exist: /mnt/nfs/ete-exstream/ete-SA/output/MFC/debbd290-4ac5-4282-9868-606ffa4caab9/archive
2025-08-18T06:54:00.424+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📂 Checking folder='email' for account 27713040001 at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/debbd290-4ac5-4282-9868-606ffa4caab9/email
2025-08-18T06:54:00.424+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Folder 'email' does not exist at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/debbd290-4ac5-4282-9868-606ffa4caab9/email
2025-08-18T06:54:00.424+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📂 Checking folder='mobstat' for account 27713040001 at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/debbd290-4ac5-4282-9868-606ffa4caab9/mobstat
2025-08-18T06:54:00.424+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Folder 'mobstat' does not exist at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/debbd290-4ac5-4282-9868-606ffa4caab9/mobstat
2025-08-18T06:54:00.424+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📂 Checking folder='print' for account 27713040001 at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/debbd290-4ac5-4282-9868-606ffa4caab9/print
2025-08-18T06:54:00.424+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ❌ Folder 'print' does not exist at path /mnt/nfs/ete-exstream/ete-SA/output/MFC/debbd290-4ac5-4282-9868-606ffa4caab9/print
2025-08-18T06:54:00.424+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ➡️ Processing customer with accountNumber=27715540001
2025-08-18T06:5


=================
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
                    logger.debug("[{}] 📄 Scanning archive folder for account {}", msg.getBatchId(), account);

                    Files.list(archivePath)
                            .forEach(f -> logger.debug("[{}]   Found archive file: {}", msg.getBatchId(), f.getFileName()));

                    Optional<Path> archiveFile = Files.list(archivePath)
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
                        Files.list(methodPath)
                                .forEach(f -> logger.debug("[{}]   Found file in {}: {}", msg.getBatchId(), folder, f.getFileName()));

                        Optional<Path> matchedFile = Files.list(methodPath)
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
