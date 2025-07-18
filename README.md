private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            KafkaMessage msg) throws IOException {

        List<String> folders = List.of("email", "archive", "mobstat", "print");
        Map<String, List<SummaryProcessedFile>> groupedMap = new LinkedHashMap<>();
        Set<String> validFolders = new HashSet<>();
        AtomicReference<String> triggerBlobUrl = new AtomicReference<>();

        // Upload .trigger file if found
        try (Stream<Path> allFiles = Files.walk(jobDir)) {
            allFiles.filter(Files::isRegularFile).forEach(path -> {
                String fileName = path.getFileName().toString();
                if (fileName.endsWith(".trigger")) {
                    try {
                        String targetPath = String.format("%s/%s/%s/%s",
                                msg.getSourceSystem(),
                                msg.getBatchId(),
                                msg.getUniqueConsumerRef(),
                                fileName);
                        byte[] content = Files.readAllBytes(path);
                        triggerBlobUrl.set(blobStorageService.uploadFile(content, targetPath));
                        logger.info("📎 Trigger file uploaded: {}", triggerBlobUrl);
                    } catch (Exception e) {
                        logger.error("❌ Failed to upload trigger file: {}", e.getMessage());
                    }
                }
            });
        }

        // Process files per available folder
        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            validFolders.add(folder);
            try (Stream<Path> files = Files.list(folderPath)) {
                for (Path filePath : files.toList()) {
                    String fileName = filePath.getFileName().toString();
                    if (fileName.endsWith(".trigger")) continue;

                    for (SummaryProcessedFile customer : customerList) {
                        String customerId = customer.getCustomerId();
                        String accountNumber = customer.getAccountNumber();
                        String key = customerId + "::" + accountNumber;

                        if (!fileName.contains(accountNumber)) continue;

                        try {
                            SummaryProcessedFile entry = buildCopy(customer);
                            String blobPath = String.format("%s/%s/%s/%s/%s",
                                    msg.getSourceSystem(),
                                    msg.getBatchId(),
                                    msg.getUniqueConsumerRef(),
                                    folder,
                                    fileName);

                            byte[] content = Files.readAllBytes(filePath);
                            String blobUrl = blobStorageService.uploadFile(content, blobPath);

                            entry.setBlobURL(decodeUrl(blobUrl));
                            entry.setStatus("SUCCESS");
                            entry.setOutputMethod(folder); // Important to retain type

                            groupedMap.computeIfAbsent(key, k -> new ArrayList<>()).add(entry);
                        } catch (Exception e) {
                            logger.error("❌ Error uploading file for {}: {}", fileName, e.getMessage());
                        }
                    }
                }
            }
        }

        // Error report handling
        Path reportDir = jobDir.resolve("report");
        Optional<Path> errorReportPath = Files.exists(reportDir)
                ? Files.list(reportDir).filter(p -> p.getFileName().toString().contains("ErrorReport")).findFirst()
                : Optional.empty();

        Map<String, Map<String, String>> errorMap = new HashMap<>();
        String errorBlobUrl = null;

        if (errorReportPath.isPresent()) {
            String content = Files.readString(errorReportPath.get());
            String errorReportBlobPath = String.format("%s/%s/%s/report/ErrorReport.txt",
                    msg.getSourceSystem(), msg.getBatchId(), msg.getUniqueConsumerRef());
            errorBlobUrl = blobStorageService.uploadFile(content, errorReportBlobPath);
            logger.info("📄 ErrorReport uploaded: {}", errorBlobUrl);
            errorMap = parseErrorReport(content);
        }

        // Final pass: add missing/failure cases
        for (SummaryProcessedFile customer : customerList) {
            String customerId = customer.getCustomerId();
            String accountNumber = customer.getAccountNumber();
            String key = customerId + "::" + accountNumber;

            List<SummaryProcessedFile> files = groupedMap.getOrDefault(key, new ArrayList<>());

            // If no files found at all
            if (files.isEmpty()) {
                SummaryProcessedFile entry = buildCopy(customer);

                boolean matchedError = errorMap.containsKey(customerId)
                        && errorMap.get(customerId).get("account").equals(accountNumber)
                        && errorMap.get(customerId).get("method").equalsIgnoreCase(customer.getOutputMethod());

                if (matchedError) {
                    entry.setStatus("FAILED");
                    entry.setStatusDescription("Marked as failed from ErrorReport");
                    entry.setBlobURL(errorBlobUrl);
                    files.add(entry);
                } else {
                    entry.setStatus("NOT_FOUND");
                    entry.setStatusDescription("No matching files found");
                    files.add(entry);
                }

                groupedMap.put(key, files);
            }
        }

        // Set overall status
        groupedMap.values().forEach(list -> list.forEach(entry -> {
            switch (entry.getStatus()) {
                case "SUCCESS" -> entry.setOverallStatus("SUCCESS");
                case "FAILED" -> entry.setOverallStatus("FAILURE");
                default -> entry.setOverallStatus("NOT_FOUND");
            }
        }));

        // Flatten final list
        return groupedMap.values().stream().flatMap(List::stream).toList();
    }

==================

private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    Map<String, ProcessedFileEntry> entryMap = new LinkedHashMap<>();
    Map<String, List<String>> statusTracker = new HashMap<>();

    for (SummaryProcessedFile file : processedList) {
        String customerId = file.getCustomerId();
        String accountNumber = file.getAccountNumber();
        String blobURL = file.getBlobURL();
        String status = file.getStatus() != null ? file.getStatus() : "UNKNOWN";

        // Skip if both URL and status are null or status is NOT_FOUND
        if ((blobURL == null && !"FAILED".equalsIgnoreCase(status)) || 
            customerId == null || accountNumber == null) {
            continue;
        }

        String key = customerId + "::" + accountNumber;

        ProcessedFileEntry entry = entryMap.computeIfAbsent(key, k -> {
            ProcessedFileEntry e = new ProcessedFileEntry();
            e.setCustomerId(customerId);
            e.setAccountNumber(accountNumber);
            return e;
        });

        // Track status for overallStatus computation
        statusTracker.computeIfAbsent(key, k -> new ArrayList<>()).add(status);

        String lowerUrl = blobURL != null ? URLDecoder.decode(blobURL, StandardCharsets.UTF_8).toLowerCase() : "";

        if (lowerUrl.contains("/email/")) {
            entry.setPdfEmailFileUrl(blobURL);
            entry.setPdfEmailFileUrlStatus(status);
        } else if (lowerUrl.contains("/archive/")) {
            entry.setPdfArchiveFileUrl(blobURL);
            entry.setPdfArchiveFileUrlStatus(status);
        } else if (lowerUrl.contains("/mobstat/")) {
            entry.setPdfMobstatFileUrl(blobURL);
            entry.setPdfMobstatFileUrlStatus(status);
        } else if (lowerUrl.contains("/print/")) {
            entry.setPrintFileUrl(blobURL);
            entry.setPrintFileUrlStatus(status);
        } else {
            // If URL is null but status is FAILED (e.g., generation failure), track placeholder
            if ("FAILED".equalsIgnoreCase(status)) {
                entry.setPdfArchiveFileUrl(null); // or skip setting fileUrl
                entry.setPdfArchiveFileUrlStatus("FAILED");
            }
        }
    }

    // Set overallStatus per customer-account group
    for (Map.Entry<String, ProcessedFileEntry> groupedEntry : entryMap.entrySet()) {
        List<String> statuses = statusTracker.getOrDefault(groupedEntry.getKey(), List.of());

        String overallStatus;
        if (statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s))) {
            overallStatus = "SUCCESS";
        } else if (statuses.stream().anyMatch(s -> "FAILED".equalsIgnoreCase(s))) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "UNKNOWN";
        }

        groupedEntry.getValue().setOverallStatus(overallStatus);
    }

    return new ArrayList<>(entryMap.values());
}

