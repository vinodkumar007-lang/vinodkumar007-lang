private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
    Map<String, Map<String, String>> errorMap = new HashMap<>();

    try {
        Optional<Path> errorReportPath = findFilePath(msg, "ErrorReport.csv");
        if (errorReportPath.isEmpty()) {
            logger.info("⚠️ No ErrorReport.csv found for batch ID {}", msg.getBatchId());
            return errorMap;
        }

        Path path = errorReportPath.get();
        logger.info("📄 Parsing ErrorReport from: {}", path.toString());

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");

                if (parts.length >= 5) {
                    // Format: acc|accNum|some_field|method|status
                    String acc = parts[0].trim();
                    String method = parts[3].trim().toUpperCase();
                    String status = parts[4].trim();
                    errorMap.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                } else if (parts.length == 4) {
                    // Format: acc|accNum|method|status
                    String acc = parts[0].trim();
                    String method = parts[2].trim().toUpperCase();
                    String status = parts[3].trim();
                    errorMap.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                } else {
                    logger.warn("❗ Skipping malformed ErrorReport line: {}", line);
                }
            }
        }

        logger.info("✅ Parsed ErrorReport.csv: {} accounts with errors", errorMap.size());
        errorMap.forEach((acc, methodMap) ->
            logger.debug("➡️ Account: {}, ErrorMethods: {}", acc, methodMap)
        );

    } catch (IOException e) {
        logger.error("❌ Failed to parse ErrorReport.csv for batch {}: {}", msg.getBatchId(), e.getMessage(), e);
    }

    return errorMap;
}

===========

private static ProcessedFileEntry buildProcessedFileEntry(
    String customerId,
    String accountNumber,
    List<SummaryProcessedFile> files,
    Map<String, String> errorMap
) {
    ProcessedFileEntry entry = new ProcessedFileEntry();
    entry.setCustomerId(customerId);
    entry.setAccountNumber(accountNumber);

    Map<String, SummaryProcessedFile> typeMap = files.stream()
        .collect(Collectors.toMap(SummaryProcessedFile::getOutputType, f -> f, (f1, f2) -> f1));

    // EMAIL
    SummaryProcessedFile email = typeMap.get("EMAIL");
    if (email != null && isValidUrl(email.getBlobUrl())) {
        entry.setPdfEmailFileUrl(email.getBlobUrl());
        entry.setPdfEmailFileUrlStatus("SUCCESS");
    } else {
        entry.setPdfEmailFileUrl(null);
        entry.setPdfEmailFileUrlStatus("NOT_FOUND");
    }

    // ARCHIVE
    SummaryProcessedFile archive = typeMap.get("ARCHIVE");
    if (archive != null && isValidUrl(archive.getBlobUrl())) {
        entry.setPdfArchiveFileUrl(archive.getBlobUrl());
        entry.setPdfArchiveFileUrlStatus("SUCCESS");
    } else {
        entry.setPdfArchiveFileUrl(null);
        entry.setPdfArchiveFileUrlStatus("NOT_FOUND");

        // Special check: if EMAIL is missing but ARCHIVE found and errorMap has message
        if (email == null || !isValidUrl(email.getBlobUrl())) {
            String key = customerId + "::" + accountNumber + "::ARCHIVE";
            if (errorMap.containsKey(key)) {
                entry.setPdfArchiveFileUrlStatus("FAILED");
            }
        }
    }

    // MOBSTAT
    SummaryProcessedFile mobstat = typeMap.get("MOBSTAT");
    if (mobstat != null && isValidUrl(mobstat.getBlobUrl())) {
        entry.setPdfMobstatFileUrl(mobstat.getBlobUrl());
        entry.setPdfMobstatFileUrlStatus("SUCCESS");
    } else {
        entry.setPdfMobstatFileUrl(null);
        entry.setPdfMobstatFileUrlStatus("NOT_FOUND");

        if (mobstat == null || !isValidUrl(mobstat.getBlobUrl())) {
            String key = customerId + "::" + accountNumber + "::MOBSTAT";
            if (errorMap.containsKey(key)) {
                entry.setPdfMobstatFileUrlStatus("FAILED");
            }
        }
    }

    // PRINT
    SummaryProcessedFile print = typeMap.get("PRINT");
    if (print != null && isValidUrl(print.getBlobUrl())) {
        entry.setPrintFileUrl(print.getBlobUrl());
        entry.setPrintFileUrlStatus("SUCCESS");
    } else {
        entry.setPrintFileUrl(null);
        entry.setPrintFileUrlStatus("NOT_FOUND");

        if (print == null || !isValidUrl(print.getBlobUrl())) {
            String key = customerId + "::" + accountNumber + "::PRINT";
            if (errorMap.containsKey(key)) {
                entry.setPrintFileUrlStatus("FAILED");
            }
        }
    }

    // Final Overall Status
    Set<String> statuses = new HashSet<>(Arrays.asList(
        entry.getPdfEmailFileUrlStatus(),
        entry.getPdfArchiveFileUrlStatus(),
        entry.getPdfMobstatFileUrlStatus(),
        entry.getPrintFileUrlStatus()
    ));

    if (statuses.size() == 1 && statuses.contains("SUCCESS")) {
        entry.setOverAllStatusCode("SUCCESS");
    } else if (statuses.stream().allMatch(s -> s.equals("NOT_FOUND") || s.equals("SUCCESS"))) {
        entry.setOverAllStatusCode("PARTIAL");
    } else {
        entry.setOverAllStatusCode("FAILED");
    }

    return entry;
}
=========
private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "archive", "ARCHIVE",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    for (SummaryProcessedFile customer : customerList) {
        String account = customer.getAccountNumber();
        String customerId = customer.getCustomerId();
        Map<String, Boolean> methodAdded = new HashMap<>();

        for (String folder : folders) {
            methodAdded.put(folder, false);
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            Optional<Path> match = Files.list(folderPath)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().contains(account))
                    .findFirst();

            if (match.isPresent()) {
                Path filePath = match.get();
                File file = filePath.toFile();
                String blobUrl = blobStorageService.uploadFileByMessage(file, folder, msg);

                SummaryProcessedFile successEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, successEntry);
                successEntry.setOutputMethod(folderToOutputMethod.get(folder));
                successEntry.setStatus("SUCCESS");
                successEntry.setBlobURL(blobUrl);

                if ("archive".equals(folder)) {
                    for (String deliveryFolder : List.of("email", "mobstat", "print")) {
                        Path deliveryPath = jobDir.resolve(deliveryFolder);
                        if (Files.exists(deliveryPath)) {
                            boolean found = Files.list(deliveryPath)
                                    .filter(Files::isRegularFile)
                                    .anyMatch(p -> p.getFileName().toString().contains(account));
                            if (found) {
                                successEntry.setLinkedDeliveryType(deliveryFolder.toUpperCase());
                                break;
                            }
                        }
                    }
                }

                finalList.add(successEntry);
                methodAdded.put(folder, true);
            }
        }

        for (String folder : folders) {
            if (methodAdded.get(folder)) continue;
            Path folderPath = jobDir.resolve(folder);
            if (Files.exists(folderPath)) {
                SummaryProcessedFile notFoundEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, notFoundEntry);
                notFoundEntry.setOutputMethod(folderToOutputMethod.get(folder));
                notFoundEntry.setStatus("NOT_FOUND");
                notFoundEntry.setStatusDescription("File not found for method: " + folder);
                notFoundEntry.setReason("File not found in " + folder + " folder");
                notFoundEntry.setBlobURL(null);

                if ("archive".equals(folder)) {
                    for (String deliveryFolder : List.of("email", "mobstat", "print")) {
                        Path deliveryPath = jobDir.resolve(deliveryFolder);
                        if (Files.exists(deliveryPath)) {
                            boolean found = Files.list(deliveryPath)
                                    .filter(Files::isRegularFile)
                                    .anyMatch(p -> p.getFileName().toString().contains(account));
                            if (found) {
                                notFoundEntry.setLinkedDeliveryType(deliveryFolder.toUpperCase());
                                break;
                            }
                        }
                    }
                }

                finalList.add(notFoundEntry);
            }
        }

        // ✅ Add ERROR_REPORT entry if exists (this is truly failed)
        if (errorMap.containsKey(account)) {
            Map<String, String> errData = errorMap.get(account);
            SummaryProcessedFile errorEntry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, errorEntry);
            errorEntry.setOutputMethod("ERROR_REPORT");
            errorEntry.setStatus("FAILED");
            errorEntry.setStatusDescription("Marked as failed from ErrorReport");
            errorEntry.setReason(errData.get("reason"));
            errorEntry.setBlobURL(errData.get("blobURL"));
            finalList.add(errorEntry);
        }
    }

    return finalList;
}

