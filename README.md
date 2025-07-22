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

==================

private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputType = file.getOutputType() != null ? file.getOutputType().toUpperCase(Locale.ROOT) : "";
        String blobUrl = file.getBlobUrl();
        String status = file.getStatus();

        String errorKey = file.getCustomerId() + "-" + file.getAccountNumber();
        boolean isErrorPresent = errorMap.containsKey(errorKey);

        // 👉 If error exists and it's EMAIL/PRINT/MOBSTAT, force status to FAILED
        if (isErrorPresent && !outputType.equals("ARCHIVE")) {
            status = "FAILED";
        }

        switch (outputType) {
            case "EMAIL":
                entry.setEmailBlobUrl(blobUrl);
                entry.setEmailStatus(status);
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(blobUrl);
                entry.setArchiveStatus(status);
                break;
            case "PRINT":
                entry.setPrintBlobUrl(blobUrl);
                entry.setPrintStatus(status);
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(blobUrl);
                entry.setMobstatStatus(status);
                break;
        }

        grouped.put(key, entry);
    }

    // ✅ Final loop to calculate overallStatus
    for (ProcessedFileEntry entry : grouped.values()) {
        String errorKey = entry.getCustomerId() + "-" + entry.getAccountNumber();
        boolean isErrorPresent = errorMap.containsKey(errorKey);

        List<String> statuses = new ArrayList<>();
        if (entry.getEmailStatus() != null) statuses.add(entry.getEmailStatus());
        if (entry.getMobstatStatus() != null) statuses.add(entry.getMobstatStatus());
        if (entry.getPrintStatus() != null) statuses.add(entry.getPrintStatus());
        if (entry.getArchiveStatus() != null) statuses.add(entry.getArchiveStatus());

        boolean allSuccess = !statuses.isEmpty() && statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s));
        boolean anyFailed = statuses.stream().anyMatch(s -> "FAILED".equalsIgnoreCase(s));
        boolean allFailed = !statuses.isEmpty() && statuses.stream().allMatch(s -> "FAILED".equalsIgnoreCase(s));

        String overallStatus;

        if (isErrorPresent) {
            // ✅ Force overallStatus = FAILED if errorMap has this account
            overallStatus = "FAILED";
        } else if (allSuccess) {
            overallStatus = "SUCCESS";
        } else if (allFailed) {
            overallStatus = "FAILED";
        } else if (anyFailed) {
            overallStatus = "PARTIAL";
        } else if ("SUCCESS".equalsIgnoreCase(entry.getArchiveStatus()) && statuses.size() == 1) {
            overallStatus = "SUCCESS";
        } else {
            overallStatus = "FAILED";
        }

        entry.setOverallStatus(overallStatus);
    }

    // ✅ File count logic (unchanged)
    long fileCount = grouped.values().stream()
            .filter(entry ->
                    "SUCCESS".equalsIgnoreCase(entry.getEmailStatus()) ||
                            "SUCCESS".equalsIgnoreCase(entry.getPrintStatus()) ||
                            "SUCCESS".equalsIgnoreCase(entry.getMobstatStatus()) ||
                            "SUCCESS".equalsIgnoreCase(entry.getArchiveStatus())
            )
            .count();

    System.out.println("Final fileCount (unique customers with at least one successful output): " + fileCount);

    return new ArrayList<>(grouped.values());
}
