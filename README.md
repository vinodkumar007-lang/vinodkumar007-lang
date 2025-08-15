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

    logger.debug("🔍 Starting buildDetailedProcessedFiles with jobDir={}, customerList size={}, msg={}",
            jobDir, (customerList != null ? customerList.size() : null), msg);

    if (jobDir == null || customerList == null || msg == null) {
        logger.warn("⚠️ One or more input parameters are null: jobDir={}, customerList={}, msg={}",
                jobDir, customerList, msg);
        return finalList;
    }

    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
    logger.debug("📂 Archive folder path: {}", archivePath);

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) {
            logger.debug("⚠️ Skipping null customer entry");
            continue;
        }

        String account = customer.getAccountNumber();
        logger.debug("➡️ Processing customer with accountNumber={}", account);

        if (account == null || account.isBlank()) {
            logger.warn("⚠️ Skipping customer with empty account number");
            continue;
        }

        // Archive upload
        String archiveBlobUrl = null;
        try {
            if (Files.exists(archivePath)) {
                logger.debug("📄 Listing files in archive folder for account {}", account);
                Files.list(archivePath).forEach(f -> logger.debug("   Found archive file: {}", f.getFileName()));

                Optional<Path> archiveFile = Files.list(archivePath)
                        .filter(Files::isRegularFile)
                        .filter(p -> {
                            boolean match = p.getFileName().toString().contains(account);
                            if (match) logger.debug("✅ Matched archive file for account {}: {}", account, p.getFileName());
                            return match;
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
                    logger.info("📦 Uploaded archive file for account {}: {}", account, archiveBlobUrl);
                } else {
                    logger.debug("❌ No archive file found for account {}", account);
                }
            } else {
                logger.debug("❌ Archive folder does not exist: {}", archivePath);
            }
        } catch (Exception e) {
            logger.warn("⚠️ Failed to upload archive file for account {}: {}", account, e.getMessage(), e);
        }

        // EMAIL, MOBSTAT, PRINT uploads
        for (String folder : deliveryFolders) {
            String outputMethod = folderToOutputMethod.get(folder);
            Path methodPath = jobDir.resolve(folder);
            String blobUrl = null;

            logger.debug("📂 Checking folder '{}' for account {} at path {}", folder, account, methodPath);

            try {
                if (Files.exists(methodPath)) {
                    Files.list(methodPath).forEach(f -> logger.debug("   Found file in {}: {}", folder, f.getFileName()));

                    Optional<Path> match = Files.list(methodPath)
                            .filter(Files::isRegularFile)
                            .filter(p -> {
                                boolean match = p.getFileName().toString().contains(account);
                                if (match) logger.debug("✅ Matched {} file for account {}: {}", folder, account, p.getFileName());
                                return match;
                            })
                            .findFirst();

                    if (match.isPresent()) {
                        blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                        logger.info("✅ Uploaded {} file for account {}: {}", outputMethod, account, blobUrl);
                    } else {
                        logger.debug("❌ No matching file found in {} for account {}", folder, account);
                    }
                } else {
                    logger.debug("❌ Folder '{}' does not exist at path {}", folder, methodPath);
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

    logger.debug("✅ buildDetailedProcessedFiles completed. Final processed list size={}", finalList.size());
    return finalList;
}

private List<CustomerSummary> parseSTDXml(File xmlFile, Map<String, Map<String, String>> errorMap) {
    List<CustomerSummary> customerSummaries = new ArrayList<>();

    try {
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = builder.parse(xmlFile);
        document.getDocumentElement().normalize();

        NodeList customerNodes = document.getElementsByTagName("customer");

        for (int i = 0; i < customerNodes.getLength(); i++) {
            Element customerElement = (Element) customerNodes.item(i);

            String accountNumber = null;
            String cisNumber = null;
            List<String> deliveryMethods = new ArrayList<>();

            NodeList keyNodes = customerElement.getElementsByTagName("key");
            for (int j = 0; j < keyNodes.getLength(); j++) {
                Element keyElement = (Element) keyNodes.item(j);
                String keyName = keyElement.getAttribute("name");

                // ✅ Match even if keyName has suffix (_MFC, _DEBITMAN, etc.)
                if (keyName != null && keyName.toLowerCase().startsWith("accountnumber")) {
                    accountNumber = keyElement.getTextContent();
                } else if (keyName != null && keyName.toLowerCase().startsWith("cisnumber")) {
                    cisNumber = keyElement.getTextContent();
                }
            }

            NodeList queueNodes = customerElement.getElementsByTagName("queueName");
            for (int q = 0; q < queueNodes.getLength(); q++) {
                String method = queueNodes.item(q).getTextContent().trim().toUpperCase();
                if (!method.isEmpty()) {
                    deliveryMethods.add(method);
                }
            }

            if (accountNumber != null && cisNumber != null) {
                CustomerSummary summary = new CustomerSummary();
                summary.setAccountNumber(accountNumber);
                summary.setCisNumber(cisNumber);
                summary.setCustomerId(accountNumber);

                Map<String, String> deliveryStatusMap = errorMap.getOrDefault(accountNumber, new HashMap<>());
                summary.setDeliveryStatus(deliveryStatusMap);

                long failedCount = deliveryMethods.stream()
                        .filter(method -> "FAILED".equalsIgnoreCase(deliveryStatusMap.getOrDefault(method, "")))
                        .count();

                if (failedCount == deliveryMethods.size()) {
                    summary.setStatus("FAILED");
                } else if (failedCount > 0) {
                    summary.setStatus("PARTIAL");
                } else {
                    summary.setStatus("SUCCESS");
                }

                customerSummaries.add(summary);

                logger.debug("📋 Customer: {}, CIS: {}, Methods: {}, Failed: {}, FinalStatus: {}",
                        accountNumber, cisNumber, deliveryMethods, failedCount, summary.getStatus());
            }
        }

    } catch (Exception e) {
        logger.error("❌ Failed parsing STD XML file: {}", xmlFile.getAbsolutePath(), e);
        throw new RuntimeException("Failed to parse XML file: " + xmlFile.getName(), e);
    }

    return customerSummaries;
}

