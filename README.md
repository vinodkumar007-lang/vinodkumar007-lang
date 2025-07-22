 private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();
        List<String> deliveryFolders = List.of("email", "mobstat", "print");
        Map<String, String> folderToOutputMethod = Map.of(
                "email", "EMAIL",
                "mobstat", "MOBSTAT",
                "print", "PRINT"
        );

        Path archivePath = jobDir.resolve("archive");

        for (SummaryProcessedFile customer : customerList) {
            String account = customer.getAccountNumber();

            // Upload archive once per customer
            String archiveBlobUrl = null;
            String archiveStatus = "NOT-FOUND";

            if (Files.exists(archivePath)) {
                Optional<Path> archiveFile = Files.list(archivePath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (archiveFile.isPresent()) {
                    archiveBlobUrl = blobStorageService.uploadFileByMessage(
                            archiveFile.get().toFile(), "archive", msg);
                    archiveStatus = "SUCCESS";

                    // ✅ Add archive entry to final list
                    SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, archiveEntry);
                    archiveEntry.setOutputType("ARCHIVE");
                    archiveEntry.setBlobUrl(archiveBlobUrl);
                    archiveEntry.setStatus(archiveStatus);
                    archiveEntry.setOverallStatus(archiveStatus); // initially, just ARCHIVE part
                    finalList.add(archiveEntry);
                }
            }

            // Process delivery types (email/mobstat/print)
            for (String folder : deliveryFolders) {
                String outputMethod = folderToOutputMethod.get(folder);
                Path methodPath = jobDir.resolve(folder);

                String blobUrl = "";
                String deliveryStatus = "SUCCESS";
                boolean fileFound = false;

                if (Files.exists(methodPath)) {
                    Optional<Path> match = Files.list(methodPath)
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (match.isPresent()) {
                        blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                        fileFound = true;
                    }
                }

                Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());

                // ✅ NEW LOGIC: mark failed if errorMap contains the account and this outputType exists
                if (customerErrors.containsKey(outputMethod) &&
                        "FAILED".equalsIgnoreCase(customerErrors.get(outputMethod))) {
                    deliveryStatus = "FAILED";
                } else if (errorMap.containsKey(account) &&
                        customerErrors.isEmpty()) {
                    // fallback if account present in errorMap but no method-specific errors
                    deliveryStatus = "FAILED";
                } else if (!fileFound) {
                    deliveryStatus = "SUCCESS"; // still success unless error explicitly present
                }

                String overallStatus = deliveryStatus;

                // Build entry
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputType(outputMethod);
                entry.setBlobUrl(blobUrl);
                entry.setStatus(deliveryStatus);

                // ✅ Also set archive fields for this entry
                entry.setArchiveOutputType("ARCHIVE");
                entry.setArchiveBlobUrl(archiveBlobUrl);
                entry.setArchiveStatus(archiveStatus);
                entry.setOverallStatus(overallStatus);

                finalList.add(entry);
            }
        }

        return finalList;
    }

    ==========================

     private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap) {

        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();

            ProcessedFileEntry entry;
            if (grouped.containsKey(key)) {
                entry = grouped.get(key); // retrieve existing
            } else {
                entry = new ProcessedFileEntry();
                entry.setCustomerId(file.getCustomerId());
                entry.setAccountNumber(file.getAccountNumber());
                grouped.put(key, entry);
            }

            String outputType = file.getOutputType().toUpperCase();
            String blobUrl = file.getBlobUrl() != null ? file.getBlobUrl() : "";

            switch (outputType) {
                case "EMAIL":
                    entry.setEmailBlobUrl(blobUrl);
                    entry.setEmailStatus(!blobUrl.isEmpty() ? "SUCCESS" : "");
                    break;
                case "PRINT":
                    entry.setPrintBlobUrl(blobUrl);
                    entry.setPrintStatus(!blobUrl.isEmpty() ? "SUCCESS" : "");
                    break;
                case "MOBSTAT":
                    entry.setMobstatBlobUrl(blobUrl);
                    entry.setMobstatStatus(!blobUrl.isEmpty() ? "SUCCESS" : "");
                    break;
                case "ARCHIVE":
                    entry.setArchiveBlobUrl(blobUrl);
                    entry.setArchiveStatus(!blobUrl.isEmpty() ? "SUCCESS" : "FAILED"); // archive mandatory
                    break;
            }
        }

        for (ProcessedFileEntry entry : grouped.values()) {
            String key = entry.getCustomerId() + "-" + entry.getAccountNumber();
            Map<String, String> errorStatusMap = errorMap.getOrDefault(key, new HashMap<>());

            if ((entry.getEmailBlobUrl() == null || entry.getEmailBlobUrl().isEmpty())
                    && "FAILED".equalsIgnoreCase(errorStatusMap.get("EMAIL"))) {
                entry.setEmailStatus("FAILED");
            }

            if ((entry.getPrintBlobUrl() == null || entry.getPrintBlobUrl().isEmpty())
                    && "FAILED".equalsIgnoreCase(errorStatusMap.get("PRINT"))) {
                entry.setPrintStatus("FAILED");
            }

            if ((entry.getMobstatBlobUrl() == null || entry.getMobstatBlobUrl().isEmpty())
                    && "FAILED".equalsIgnoreCase(errorStatusMap.get("MOBSTAT"))) {
                entry.setMobstatStatus("FAILED");
            }

            // Determine overall status
            boolean allEmpty = Stream.of(
                    entry.getEmailStatus(),
                    entry.getPrintStatus(),
                    entry.getMobstatStatus()
            ).allMatch(s -> s == null || s.isEmpty());

            boolean allSuccess = Stream.of(
                    entry.getEmailStatus(),
                    entry.getPrintStatus(),
                    entry.getMobstatStatus()
            ).allMatch(s -> "SUCCESS".equalsIgnoreCase(s) || s == null || s.isEmpty());

            boolean anyFailed = Stream.of(
                    entry.getEmailStatus(),
                    entry.getPrintStatus(),
                    entry.getMobstatStatus()
            ).anyMatch(s -> "FAILED".equalsIgnoreCase(s));

            if (allEmpty || allSuccess) {
                entry.setOverallStatus("SUCCESS");
            } else if (anyFailed && !allEmpty) {
                entry.setOverallStatus("FAILED");
            } else {
                entry.setOverallStatus("PARTIAL");
            }
        }

        return new ArrayList<>(grouped.values());
    }


============

private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();

        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());
        grouped.putIfAbsent(key, entry);

        String outputType = file.getOutputType().toUpperCase();
        String blobUrl = file.getBlobUrl() != null ? file.getBlobUrl() : "";

        switch (outputType) {
            case "EMAIL":
                entry.setEmailBlobUrl(blobUrl);
                entry.setEmailStatus(!blobUrl.isEmpty() ? "SUCCESS" : "");
                break;
            case "PRINT":
                entry.setPrintBlobUrl(blobUrl);
                entry.setPrintStatus(!blobUrl.isEmpty() ? "SUCCESS" : "");
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(blobUrl);
                entry.setMobstatStatus(!blobUrl.isEmpty() ? "SUCCESS" : "");
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(blobUrl);
                entry.setArchiveStatus(!blobUrl.isEmpty() ? "SUCCESS" : "FAILED"); // archive is always expected
                break;
        }
    }

    for (ProcessedFileEntry entry : grouped.values()) {
        String key = entry.getCustomerId() + "-" + entry.getAccountNumber();
        Map<String, String> errorStatusMap = errorMap.getOrDefault(key, new HashMap<>());

        // EMAIL
        if (isEmpty(entry.getEmailStatus())) {
            if ("FAILED".equalsIgnoreCase(errorStatusMap.get("EMAIL"))) {
                entry.setEmailStatus("FAILED");
            } else if (errorStatusMap.containsKey("EMAIL")) {
                entry.setEmailStatus("NOT_FOUND");
            }
        }

        // PRINT
        if (isEmpty(entry.getPrintStatus())) {
            if ("FAILED".equalsIgnoreCase(errorStatusMap.get("PRINT"))) {
                entry.setPrintStatus("FAILED");
            } else if (errorStatusMap.containsKey("PRINT")) {
                entry.setPrintStatus("NOT_FOUND");
            }
        }

        // MOBSTAT
        if (isEmpty(entry.getMobstatStatus())) {
            if ("FAILED".equalsIgnoreCase(errorStatusMap.get("MOBSTAT"))) {
                entry.setMobstatStatus("FAILED");
            } else if (errorStatusMap.containsKey("MOBSTAT")) {
                entry.setMobstatStatus("NOT_FOUND");
            }
        }

        // Default empty marking if nothing at all found
        if (entry.getEmailStatus() == null) entry.setEmailStatus("");
        if (entry.getPrintStatus() == null) entry.setPrintStatus("");
        if (entry.getMobstatStatus() == null) entry.setMobstatStatus("");

        // OVERALL STATUS LOGIC
        List<String> statuses = Arrays.asList(
            entry.getEmailStatus(),
            entry.getPrintStatus(),
            entry.getMobstatStatus()
        );

        boolean allEmpty = statuses.stream().allMatch(s -> s == null || s.isEmpty());
        boolean allSuccess = statuses.stream().allMatch(s -> s.equals("SUCCESS") || s.isEmpty());
        boolean anyFailed = statuses.stream().anyMatch(s -> s.equals("FAILED"));
        boolean anyNotFound = statuses.stream().anyMatch(s -> s.equals("NOT_FOUND"));

        if (allEmpty) {
            entry.setOverallStatus("PARTIAL");
        } else if (allSuccess) {
            entry.setOverallStatus("SUCCESS");
        } else if (anyFailed) {
            entry.setOverallStatus("FAILED");
        } else if (anyNotFound) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("PARTIAL");
        }
    }

    return new ArrayList<>(grouped.values());
}

private static boolean isEmpty(String str) {
    return str == null || str.trim().isEmpty();
}
