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

                String acc;
                String method;
                String status;

                if (parts.length >= 5) {
                    // Format: acc|accNum|some_field|method|status
                    acc = parts[0].trim();
                    method = parts[3].trim().toUpperCase();
                    status = parts[4].trim();
                } else if (parts.length == 4) {
                    // Format: acc|accNum|method|status
                    acc = parts[0].trim();
                    method = parts[2].trim().toUpperCase();
                    status = parts[3].trim();
                } else {
                    logger.warn("❗ Skipping malformed ErrorReport line: {}", line);
                    continue;
                }

                // Normalize method name to EMAIL, ARCHIVE, MOBSTAT, PRINT
                if (method.equalsIgnoreCase("EMAIL") || method.equalsIgnoreCase("ARCHIVE")
                        || method.equalsIgnoreCase("MOBSTAT") || method.equalsIgnoreCase("PRINT")) {

                    // If status is FAILED, mark it; else, mark it as NOT_FOUND (to indicate presence without failure)
                    if (status.equalsIgnoreCase("FAILED")) {
                        errorMap.computeIfAbsent(acc, k -> new HashMap<>()).put(method, "FAILED");
                    } else {
                        errorMap.computeIfAbsent(acc, k -> new HashMap<>()).putIfAbsent(method, "NOT_FOUND");
                    }
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


==

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
        boolean hasAtLeastOneSuccess = false;
        Map<String, Boolean> methodAdded = new HashMap<>();

        for (String folder : folders) {
            methodAdded.put(folder, false); // to prevent duplicates
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
                hasAtLeastOneSuccess = true;
                methodAdded.put(folder, true);
            }
        }

        for (String folder : folders) {
            if (methodAdded.get(folder)) continue;

            Path folderPath = jobDir.resolve(folder);
            if (Files.exists(folderPath)) {
                SummaryProcessedFile failedEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, failedEntry);
                failedEntry.setOutputMethod(folderToOutputMethod.get(folder));

                if (errorMap.containsKey(account)) {
                    // File not found + error report = FAILED
                    Map<String, String> err = errorMap.get(account);
                    failedEntry.setStatus("FAILED");
                    failedEntry.setStatusDescription("Marked as failed from ErrorReport");
                    failedEntry.setReason(err.getOrDefault("reason", "ErrorReport reason not available"));
                    failedEntry.setBlobURL(err.getOrDefault("blobURL", null));
                } else {
                    // File not found but no error report = NOT_FOUND
                    failedEntry.setStatus("NOT_FOUND");
                    failedEntry.setStatusDescription("File not found for method: " + folder);
                    failedEntry.setReason("File not found in " + folder + " folder");
                    failedEntry.setBlobURL(null);
                }

                if ("archive".equals(folder)) {
                    for (String deliveryFolder : List.of("email", "mobstat", "print")) {
                        Path deliveryPath = jobDir.resolve(deliveryFolder);
                        if (Files.exists(deliveryPath)) {
                            boolean found = Files.list(deliveryPath)
                                    .filter(Files::isRegularFile)
                                    .anyMatch(p -> p.getFileName().toString().contains(account));
                            if (found) {
                                failedEntry.setLinkedDeliveryType(deliveryFolder.toUpperCase());
                                break;
                            }
                        }
                    }
                }

                finalList.add(failedEntry);
            }
        }

        // Add explicit ERROR_REPORT if not already added above
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

=========
private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> group : grouped.entrySet()) {
        String[] parts = group.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        Map<String, SummaryProcessedFile> methodMap = new HashMap<>();
        Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();

        for (SummaryProcessedFile file : group.getValue()) {
            String method = file.getOutputMethod();
            if (method == null) continue;

            switch (method.toUpperCase()) {
                case "EMAIL", "MOBSTAT", "PRINT" -> methodMap.put(method.toUpperCase(), file);
                case "ARCHIVE" -> {
                    String linked = file.getLinkedDeliveryType();
                    if (linked != null) {
                        archiveMap.put(linked.toUpperCase(), file);
                    }
                }
            }
        }

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);

        List<String> statuses = new ArrayList<>();
        boolean hasAtLeastOneSuccess = false;
        boolean hasAtLeastOnePresent = false;

        for (String type : List.of("EMAIL", "MOBSTAT", "PRINT")) {
            SummaryProcessedFile delivery = methodMap.get(type);
            SummaryProcessedFile archive = archiveMap.get(type);

            String deliveryStatus = null, archiveStatus = null;

            if (delivery != null) {
                hasAtLeastOnePresent = true;
                deliveryStatus = delivery.getStatus();
                String url = delivery.getBlobURL();
                String reason = delivery.getStatusDescription();

                switch (type) {
                    case "EMAIL" -> {
                        entry.setPdfEmailFileUrl(url);
                        entry.setPdfEmailFileUrlStatus(deliveryStatus);
                        if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                    }
                    case "MOBSTAT" -> {
                        entry.setPdfMobstatFileUrl(url);
                        entry.setPdfMobstatFileUrlStatus(deliveryStatus);
                        if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                    }
                    case "PRINT" -> {
                        entry.setPrintFileUrl(url);
                        entry.setPrintFileUrlStatus(deliveryStatus);
                        if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                    }
                }

                if ("SUCCESS".equalsIgnoreCase(deliveryStatus)) hasAtLeastOneSuccess = true;
            }

            if (archive != null) {
                hasAtLeastOnePresent = true;
                archiveStatus = archive.getStatus();
                String url = archive.getBlobURL();
                entry.setPdfArchiveFileUrl(url); // Shared field
                entry.setPdfArchiveFileUrlStatus(archiveStatus);
                if ("FAILED".equalsIgnoreCase(archiveStatus) && entry.getReason() == null) {
                    entry.setReason(archive.getStatusDescription());
                }
                if ("SUCCESS".equalsIgnoreCase(archiveStatus)) hasAtLeastOneSuccess = true;
            }

            // Handle case where one or both exist
            if (delivery != null || archive != null) {
                if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                    statuses.add("SUCCESS");
                } else if ("FAILED".equalsIgnoreCase(deliveryStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
                    statuses.add("FAILED");
                } else {
                    statuses.add("PARTIAL");
                }
            }
        }

        if (!hasAtLeastOnePresent) {
            // No delivery/archive at all — skip
            continue;
        }

        if (!hasAtLeastOneSuccess) {
            // Files attempted, all failed
            entry.setOverAllStatusCode("FAILED");
        } else if (statuses.stream().allMatch(s -> "SUCCESS".equals(s))) {
            entry.setOverAllStatusCode("SUCCESS");
        } else {
            entry.setOverAllStatusCode("PARTIAL");
        }

        finalList.add(entry);
    }

    return finalList;
}
