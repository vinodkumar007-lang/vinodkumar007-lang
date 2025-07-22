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

        // Step 1: Get archive blob (always needed)
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

        // Step 2: Process each delivery method (EMAIL, MOBSTAT, PRINT)
        for (String folder : deliveryFolders) {
            String method = folderToOutputMethod.get(folder);
            Path methodPath = jobDir.resolve(folder);

            String deliveryBlobUrl = null;
            String deliveryStatus;
            boolean fileFound = false;

            // Check if file exists
            if (Files.exists(methodPath)) {
                Optional<Path> match = Files.list(methodPath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (match.isPresent()) {
                    deliveryBlobUrl = blobStorageService.uploadFileByMessage(
                            match.get().toFile(), folder, msg);
                    deliveryStatus = "SUCCESS";
                    fileFound = true;
                }
            }

            // Not found in folder, check errorMap
            if (!fileFound) {
                Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());
                String errorStatus = customerErrors.getOrDefault(method, null);

                if ("FAILED".equalsIgnoreCase(errorStatus)) {
                    deliveryStatus = "FAILED";
                } else {
                    deliveryStatus = "NOT-FOUND";
                }
            }

            // Step 3: Calculate overallStatus
            String overallStatus;
            if ("SUCCESS".equals(deliveryStatus) && archiveBlobUrl != null) {
                overallStatus = "SUCCESS";
            } else if ("FAILED".equals(deliveryStatus)) {
                overallStatus = "FAILED";
            } else {
                overallStatus = "PARTIAL";
            }

            // Step 4: Create combined object
            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setOutputType(method);
            entry.setBlobURL(deliveryBlobUrl);
            entry.setStatus(deliveryStatus);
            entry.setArchiveOutputType("ARCHIVE");
            entry.setArchiveBlobUrl(archiveBlobUrl);
            entry.setArchiveStatus("SUCCESS"); // always present
            entry.setOverallStatus(overallStatus);

            finalList.add(entry);
        }
    }

    return finalList;
}


private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<CustomerAccountEntry> customerAccounts,
        Map<String, Path> foundFilesMap,
        Map<String, String> errorMap,
        String archiveFolderBlobUrl
) {
    List<ProcessedFileEntry> processedFileEntries = new ArrayList<>();

    for (CustomerAccountEntry entry : customerAccounts) {
        String customerId = entry.getCustomerId();
        String accountNumber = entry.getAccountNumber();
        String key = customerId + "_" + accountNumber;
        List<String> outputTypes = Arrays.asList("EMAIL", "MOBSTAT", "PRINT");

        for (String outputType : outputTypes) {
            String fileKey = key + "_" + outputType;
            ProcessedFileEntry processedEntry = new ProcessedFileEntry();
            processedEntry.setCustomerId(customerId);
            processedEntry.setAccountNumber(accountNumber);

            // Output Type specific block (EMAIL, MOBSTAT, PRINT)
            processedEntry.setOutputType(outputType);

            // ===== Set output file URL and status =====
            if (foundFilesMap.containsKey(fileKey)) {
                // File found
                String blobUrl = getBlobUrlForOutputType(outputType, fileKey); // implement as per your logic
                processedEntry.setBlobUrl(blobUrl);
                processedEntry.setStatus("SUCCESS");
            } else {
                // File not found — check errorMap
                String err = errorMap.get(fileKey);
                if (err != null && err.equalsIgnoreCase(outputType)) {
                    processedEntry.setStatus("FAILED");
                } else {
                    processedEntry.setStatus("NOT-FOUND");
                }
                processedEntry.setBlobUrl(""); // no file
            }

            // ===== Always set ARCHIVE =====
            processedEntry.setArchiveOutputType("ARCHIVE");
            processedEntry.setArchiveBlobUrl(archiveFolderBlobUrl + "/" + key + ".pdf");
            processedEntry.setArchiveStatus("SUCCESS");

            // ===== Determine overallStatus =====
            String finalStatus = processedEntry.getStatus();
            if ("SUCCESS".equals(finalStatus)) {
                processedEntry.setOverallStatus("SUCCESS");
            } else if ("FAILED".equals(finalStatus)) {
                processedEntry.setOverallStatus("FAILED");
            } else {
                processedEntry.setOverallStatus("PARTIAL");
            }

            processedFileEntries.add(processedEntry);
        }
    }

    return processedFileEntries;
}
