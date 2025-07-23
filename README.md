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

        // Archive upload
        String archiveBlobUrl = null;

        if (Files.exists(archivePath)) {
            Optional<Path> archiveFile = Files.list(archivePath)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().contains(account))
                    .findFirst();

            if (archiveFile.isPresent()) {
                archiveBlobUrl = blobStorageService.uploadFileByMessage(
                        archiveFile.get().toFile(), "archive", msg);

                SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, archiveEntry);
                archiveEntry.setOutputType("ARCHIVE");
                archiveEntry.setBlobUrl(archiveBlobUrl);

                finalList.add(archiveEntry);
            }
        }

        // EMAIL, MOBSTAT, PRINT
        for (String folder : deliveryFolders) {
            String outputMethod = folderToOutputMethod.get(folder);
            Path methodPath = jobDir.resolve(folder);

            String blobUrl = null;

            if (Files.exists(methodPath)) {
                Optional<Path> match = Files.list(methodPath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (match.isPresent()) {
                    blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                }
            }

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setOutputType(outputMethod);
            entry.setBlobUrl(blobUrl);

            if (archiveBlobUrl != null) {
                entry.setArchiveOutputType("ARCHIVE");
                entry.setArchiveBlobUrl(archiveBlobUrl);
            }

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
        ProcessedFileEntry entry = grouped.computeIfAbsent(key, k -> {
            ProcessedFileEntry newEntry = new ProcessedFileEntry();
            newEntry.setCustomerId(file.getCustomerId());
            newEntry.setAccountNumber(file.getAccountNumber());
            return newEntry;
        });

        String outputType = file.getOutputType();
        String blobUrl = file.getBlobUrl();
        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

        String status;
        if (isNonEmpty(blobUrl)) {
            status = "SUCCESS";
        } else if ("FAILED".equalsIgnoreCase(errors.getOrDefault(outputType, ""))) {
            status = "FAILED";
        } else {
            status = "NOT_FOUND";
        }

        switch (outputType) {
            case "EMAIL" -> {
                entry.setEmailUrl(blobUrl);
                entry.setEmailStatus(status);
            }
            case "PRINT" -> {
                entry.setPrintUrl(blobUrl);
                entry.setPrintStatus(status);
            }
            case "MOBSTAT" -> {
                entry.setMobstatUrl(blobUrl);
                entry.setMobstatStatus(status);
            }
            case "ARCHIVE" -> {
                entry.setArchiveUrl(blobUrl);
                entry.setArchiveStatus(status);
            }
        }
    }

    // Compute overallStatus
    for (ProcessedFileEntry entry : grouped.values()) {
        String email = entry.getEmailStatus();
        String archive = entry.getArchiveStatus();

        if ("SUCCESS".equals(email) && "SUCCESS".equals(archive)) {
            entry.setOverallStatus("SUCCESS");
        } else if ("FAILED".equals(email) && "FAILED".equals(archive)) {
            entry.setOverallStatus("FAILED");
        } else if ("SUCCESS".equals(archive) && ("FAILED".equals(email) || "NOT_FOUND".equals(email))) {
            entry.setOverallStatus("PARTIAL");
        } else if ("SUCCESS".equals(archive)) {
            entry.setOverallStatus("SUCCESS");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(grouped.values());
}

private static boolean isNonEmpty(String value) {
    return value != null && !value.trim().isEmpty();
}
