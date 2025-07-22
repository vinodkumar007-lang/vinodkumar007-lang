private static List<SummaryProcessedFile> buildDetailedProcessedFileList(
        List<CustomerDetails> customers,
        Path jobDir,
        Map<String, Map<String, String>> errorMap,
        String archiveBlobUrl,
        String archiveStatus,
        MessageDTO msg,
        BlobStorageService blobStorageService
) {
    List<SummaryProcessedFile> finalList = new ArrayList<>();

    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    List<String> deliveryFolders = List.of("email", "mobstat", "print");

    for (CustomerDetails customer : customers) {
        String account = customer.getAccountNumber();

        for (String folder : deliveryFolders) {
            String outputMethod = folderToOutputMethod.get(folder);
            Path methodPath = jobDir.resolve(folder);

            String blobUrl = "";
            String deliveryStatus = "SUCCESS";
            boolean fileFound = false;

            // 🔍 Search file in folder
            if (Files.exists(methodPath)) {
                try {
                    Optional<Path> match = Files.list(methodPath)
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (match.isPresent()) {
                        blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                        fileFound = true;
                    }
                } catch (IOException e) {
                    // your existing error logging can remain here
                    e.printStackTrace();
                }
            }

            // ✅ Updated Status Logic
            if (errorMap.containsKey(account)) {
                deliveryStatus = "FAILED";
            } else if (!fileFound) {
                deliveryStatus = "SUCCESS"; // Still success if not found and not in errorMap
            }

            String overallStatus = "FAILED".equalsIgnoreCase(deliveryStatus) ? "FAILED" : deliveryStatus;

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setOutputType(outputMethod);
            entry.setBlobUrl(blobUrl);
            entry.setStatus(deliveryStatus);
            entry.setArchiveOutputType("ARCHIVE");
            entry.setArchiveBlobUrl(archiveBlobUrl);
            entry.setArchiveStatus(archiveStatus);
            entry.setOverallStatus(overallStatus);

            finalList.add(entry);
        }
    }

    return finalList;
}


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

    // Final loop to calculate overallStatus
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

        if (allSuccess) {
            overallStatus = "SUCCESS";
        } else if (allFailed) {
            overallStatus = "FAILED";
        } else if (anyFailed) {
            overallStatus = "PARTIAL";
        } else if ("SUCCESS".equalsIgnoreCase(entry.getArchiveStatus()) && statuses.size() == 1) {
            overallStatus = "SUCCESS";
        } else {
            overallStatus = isErrorPresent ? "FAILED" : "FAILED";
        }

        entry.setOverallStatus(overallStatus);
    }

    // ✅ File count logic without touching above
    long fileCount = grouped.values().stream()
            .filter(entry ->
                    "SUCCESS".equalsIgnoreCase(entry.getEmailStatus()) ||
                    "SUCCESS".equalsIgnoreCase(entry.getPrintStatus()) ||
                    "SUCCESS".equalsIgnoreCase(entry.getMobstatStatus()) ||
                    "SUCCESS".equalsIgnoreCase(entry.getArchiveStatus())
            )
            .count();

    // Optionally: Log or return fileCount via wrapper or store elsewhere if needed
    System.out.println("Final fileCount (unique customers with at least one successful output): " + fileCount);

    return new ArrayList<>(grouped.values());
}
