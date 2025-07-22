private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles) {
    Map<String, List<SummaryProcessedFile>> grouped = processedFiles.stream()
            .collect(Collectors.groupingBy(p -> p.getCustomerId() + "|" + p.getAccountNumber()));

    List<ProcessedFileEntry> result = new ArrayList<>();

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        List<SummaryProcessedFile> groupList = entry.getValue();

        if (groupList.isEmpty()) continue;

        SummaryProcessedFile first = groupList.get(0);

        ProcessedFileEntry processedEntry = new ProcessedFileEntry();
        processedEntry.setCustomerId(first.getCustomerId());
        processedEntry.setAccountNumber(first.getAccountNumber());

        String overallStatus = "SUCCESS";

        for (SummaryProcessedFile file : groupList) {
            switch (file.getOutputType()) {
                case "EMAIL":
                    processedEntry.setEmailUrl(file.getBlobURL());
                    processedEntry.setEmailStatus(file.getStatus());
                    break;
                case "MOBSTAT":
                    processedEntry.setMobstatUrl(file.getBlobURL());
                    processedEntry.setMobstatStatus(file.getStatus());
                    break;
                case "PRINT":
                    processedEntry.setPrintUrl(file.getBlobURL());
                    processedEntry.setPrintStatus(file.getStatus());
                    break;
            }

            // Always one archive per group
            processedEntry.setArchiveUrl(file.getArchiveBlobUrl());
            processedEntry.setArchiveStatus(file.getArchiveStatus());

            // Compute overall status (any FAILED → FAILED, any PARTIAL → PARTIAL)
            if ("FAILED".equalsIgnoreCase(file.getStatus())) {
                overallStatus = "FAILED";
            } else if ("PARTIAL".equalsIgnoreCase(file.getStatus()) && !"FAILED".equalsIgnoreCase(overallStatus)) {
                overallStatus = "PARTIAL";
            }
        }

        processedEntry.setOverallStatus(overallStatus);
        result.add(processedEntry);
    }

    return result;
}

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
        String customerId = customer.getCustomerId();

        // ARCHIVE always added
        String archiveBlobUrl = null;
        if (Files.exists(archivePath)) {
            Optional<Path> archiveFile = Files.list(archivePath)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().contains(account))
                    .findFirst();

            if (archiveFile.isPresent()) {
                archiveBlobUrl = blobStorageService.uploadFileByMessage(
                        archiveFile.get().toFile(), "archive", msg);
            }
        }

        // Process EMAIL, MOBSTAT, PRINT individually
        for (String folder : deliveryFolders) {
            String outputMethod = folderToOutputMethod.get(folder);
            Path methodPath = jobDir.resolve(folder);

            String blobUrl = null;
            String deliveryStatus = "NOT-FOUND";
            boolean fileFound = false;

            if (Files.exists(methodPath)) {
                Optional<Path> match = Files.list(methodPath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (match.isPresent()) {
                    blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                    deliveryStatus = "SUCCESS";
                    fileFound = true;
                }
            }

            if (!fileFound) {
                Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());
                String errorStatus = customerErrors.getOrDefault(outputMethod, null);

                if ("FAILED".equalsIgnoreCase(errorStatus)) {
                    deliveryStatus = "FAILED";
                }
            }

            // Decide overallStatus
            String overallStatus;
            if ("SUCCESS".equals(deliveryStatus) && archiveBlobUrl != null) {
                overallStatus = "SUCCESS";
            } else if ("FAILED".equals(deliveryStatus)) {
                overallStatus = "FAILED";
            } else {
                overallStatus = "PARTIAL";
            }

            // Build and add final entry
            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setOutputType(outputMethod);
            entry.setBlobURL(blobUrl);
            entry.setStatus(deliveryStatus);
            entry.setArchiveOutputType("ARCHIVE");
            entry.setArchiveBlobUrl(archiveBlobUrl);
            entry.setArchiveStatus("SUCCESS"); // always present if archiveBlobUrl present
            entry.setOverallStatus(overallStatus);

            finalList.add(entry);
        }
    }

    return finalList;
}
