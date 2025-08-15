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

        if (jobDir == null || customerList == null || msg == null) {
            logger.warn("⚠️ One or more input parameters are null: jobDir={}, customerList={}, msg={}",
                    jobDir, customerList, msg);
            return finalList;
        }

        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);

        for (SummaryProcessedFile customer : customerList) {
            if (customer == null) continue;

            String account = customer.getAccountNumber();
            if (account == null || account.isBlank()) {
                logger.warn("⚠️ Skipping customer with empty account number");
                continue;
            }

            // Archive upload
            String archiveBlobUrl = null;
            try {
                if (Files.exists(archivePath)) {
                    Optional<Path> archiveFile = Files.list(archivePath)
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (archiveFile.isPresent()) {
                        archiveBlobUrl = blobStorageService.uploadFileByMessage(
                                archiveFile.get().toFile(), AppConstants.FOLDER_ARCHIVE, msg);

                        SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, archiveEntry);
                        archiveEntry.setOutputType(AppConstants.OUTPUT_ARCHIVE);
                        archiveEntry.setBlobUrl(decodeUrl(archiveBlobUrl));

                        finalList.add(archiveEntry);
                        logger.info("📦 Uploaded archive file for account {}: {}", account, archiveBlobUrl);
                    }
                }
            } catch (Exception e) {
                logger.warn("⚠️ Failed to upload archive file for account {}: {}", account, e.getMessage(), e);
            }

            // EMAIL, MOBSTAT, PRINT uploads
            for (String folder : deliveryFolders) {
                String outputMethod = folderToOutputMethod.get(folder);
                Path methodPath = jobDir.resolve(folder);
                String blobUrl = null;

                try {
                    if (Files.exists(methodPath)) {
                        Optional<Path> match = Files.list(methodPath)
                                .filter(Files::isRegularFile)
                                .filter(p -> p.getFileName().toString().contains(account))
                                .findFirst();

                        if (match.isPresent()) {
                            blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                            logger.info("✅ Uploaded {} file for account {}: {}", outputMethod, account, blobUrl);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("⚠️ Failed to upload {} file for account {}: {}", outputMethod, account, e.getMessage(), e);
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

        return finalList;
    }
