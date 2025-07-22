private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<CustomerAccountEntry> customerAccounts,
        Map<String, Path> foundFilesMap,
        Map<String, String> errorMap,
        String archiveFolderBlobUrl
) {
    List<ProcessedFileEntry> processedFileEntries = new ArrayList<>();
    List<String> outputTypes = Arrays.asList("EMAIL", "MOBSTAT", "PRINT");

    for (CustomerAccountEntry entry : customerAccounts) {
        String customerId = entry.getCustomerId();
        String accountNumber = entry.getAccountNumber();
        String key = customerId + "_" + accountNumber;

        for (String outputType : outputTypes) {
            String fileKey = key + "_" + outputType;

            ProcessedFileEntry processedEntry = new ProcessedFileEntry();
            processedEntry.setCustomerId(customerId);
            processedEntry.setAccountNumber(accountNumber);
            processedEntry.setOutputType(outputType);

            // === Output File: Blob URL and Status ===
            if (foundFilesMap.containsKey(fileKey)) {
                String blobUrl = getBlobUrlForOutputType(outputType, fileKey);
                processedEntry.setBlobUrl(blobUrl);
                processedEntry.setStatus("SUCCESS");
            } else {
                String errorOutput = errorMap.get(fileKey);
                if (errorOutput != null && errorOutput.equalsIgnoreCase(outputType)) {
                    processedEntry.setStatus("FAILED");
                } else {
                    processedEntry.setStatus("NOT-FOUND");
                }
                processedEntry.setBlobUrl(""); // no file
            }

            // === Archive Block: Always present ===
            processedEntry.setArchiveOutputType("ARCHIVE");
            processedEntry.setArchiveBlobUrl(archiveFolderBlobUrl + "/" + key + ".pdf");
            processedEntry.setArchiveStatus("SUCCESS");

            // === Overall Status ===
            String mainStatus = processedEntry.getStatus();
            if ("SUCCESS".equals(mainStatus)) {
                processedEntry.setOverallStatus("SUCCESS");
            } else if ("FAILED".equals(mainStatus)) {
                processedEntry.setOverallStatus("FAILED");
            } else {
                processedEntry.setOverallStatus("PARTIAL");
            }

            processedFileEntries.add(processedEntry);
        }
    }

    return processedFileEntries;
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

        // ARCHIVE: always try to find
        String archiveBlobUrl = null;
        String archiveStatus = "FAILED";
        if (Files.exists(archivePath)) {
            Optional<Path> archiveFile = Files.list(archivePath)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().contains(account))
                    .findFirst();

            if (archiveFile.isPresent()) {
                archiveBlobUrl = blobStorageService.uploadFileByMessage(archiveFile.get().toFile(), "archive", msg);
                archiveStatus = "SUCCESS";
            }
        }

        // Loop over delivery types (EMAIL, MOBSTAT, PRINT)
        for (String folder : deliveryFolders) {
            String method = folderToOutputMethod.get(folder);
            Path methodPath = jobDir.resolve(folder);

            String deliveryBlobUrl = null;
            String deliveryStatus = "NOT-FOUND";
            boolean fileFound = false;

            if (Files.exists(methodPath)) {
                Optional<Path> match = Files.list(methodPath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (match.isPresent()) {
                    deliveryBlobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                    deliveryStatus = "SUCCESS";
                    fileFound = true;
                }
            }

            // If not found in folder, check error map
            if (!fileFound) {
                Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());
                String errorStatus = customerErrors.getOrDefault(method, null);
                if ("FAILED".equalsIgnoreCase(errorStatus)) {
                    deliveryStatus = "FAILED";
                }
            }

            // Compute overallStatus based on delivery + archive
            String overallStatus;
            if ("SUCCESS".equals(deliveryStatus) && "SUCCESS".equals(archiveStatus)) {
                overallStatus = "SUCCESS";
            } else if ("FAILED".equals(deliveryStatus)) {
                overallStatus = "FAILED";
            } else if ("NOT-FOUND".equals(deliveryStatus) && "SUCCESS".equals(archiveStatus)) {
                // Email not found but archive present = overall failed
                overallStatus = "FAILED";
            } else {
                overallStatus = "PARTIAL";
            }

            // Build the final entry
            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setOutputType(method);
            entry.setBlobURL(deliveryBlobUrl);
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
