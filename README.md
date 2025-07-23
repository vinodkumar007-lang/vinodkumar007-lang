private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputType = file.getOutputType();
        String blobUrl = file.getBlobUrl();

        switch (outputType) {
            case "EMAIL":
                if (!isNonEmpty(entry.getEmailBlobUrl())) {
                    entry.setEmailBlobUrl(blobUrl);
                }
                break;
            case "ARCHIVE":
                if (!isNonEmpty(entry.getArchiveBlobUrl())) {
                    entry.setArchiveBlobUrl(blobUrl);
                }
                break;
            case "PRINT":
                if (!isNonEmpty(entry.getPrintBlobUrl())) {
                    entry.setPrintBlobUrl(blobUrl);
                }
                break;
            case "MOBSTAT":
                if (!isNonEmpty(entry.getMobstatBlobUrl())) {
                    entry.setMobstatBlobUrl(blobUrl);
                }
                break;
        }

        grouped.put(key, entry);
    }

    for (Map.Entry<String, ProcessedFileEntry> group : grouped.entrySet()) {
        String key = group.getKey();
        ProcessedFileEntry entry = group.getValue();
        Map<String, String> errorMapForKey = errorMap.getOrDefault(key, Collections.emptyMap());

        // EMAIL
        if (isNonEmpty(entry.getEmailBlobUrl())) {
            entry.setEmailStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("EMAIL")) {
            entry.setEmailStatus("FAILED");
        } else {
            entry.setEmailStatus("");
        }

        // ARCHIVE
        if (isNonEmpty(entry.getArchiveBlobUrl())) {
            entry.setArchiveStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("ARCHIVE")) {
            entry.setArchiveStatus("FAILED");
        } else {
            entry.setArchiveStatus("");
        }

        // PRINT
        if (isNonEmpty(entry.getPrintBlobUrl())) {
            entry.setPrintStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("PRINT")) {
            entry.setPrintStatus("FAILED");
        } else {
            entry.setPrintStatus("");
        }

        // MOBSTAT
        if (isNonEmpty(entry.getMobstatBlobUrl())) {
            entry.setMobstatStatus("SUCCESS");
        } else if (errorMapForKey.containsKey("MOBSTAT")) {
            entry.setMobstatStatus("FAILED");
        } else {
            entry.setMobstatStatus("");
        }

        setOverallStatus(entry);
    }

    return new ArrayList<>(grouped.values());
}

private static boolean isNonEmpty(String val) {
    return val != null && !val.trim().isEmpty();
}

=========
private static String determineOverallStatus(ProcessedFileEntry entry) {
    String email = safeStatus(entry.getEmailStatus());
    String print = safeStatus(entry.getPrintStatus());
    String mobstat = safeStatus(entry.getMobstatStatus());
    String archive = safeStatus(entry.getArchiveStatus());

    int successCount = 0;
    int failedCount = 0;
    int notFoundCount = 0;

    List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);
    for (String status : allStatuses) {
        if ("SUCCESS".equalsIgnoreCase(status)) {
            successCount++;
        } else if ("FAILED".equalsIgnoreCase(status)) {
            failedCount++;
        } else {
            // Covers "" or null or unknown statuses
            notFoundCount++;
        }
    }

    // ✅ All 4 methods successful
    if (successCount == 4) {
        return "SUCCESS";
    }

    // ❌ At least one failed, none success
    if (failedCount > 0 && successCount == 0) {
        return "FAILED";
    }

    // ⚠️ At least one success, and at least one failed or not-found
    if (successCount > 0 && (failedCount > 0 || notFoundCount > 0)) {
        return "PARTIAL";
    }

    // ❌ All methods are not found or empty
    if (notFoundCount == 4) {
        return "FAILED";
    }

    // Fallback
    return "PARTIAL";
}
=======

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
        String archiveStatus = "";

        if (Files.exists(archivePath)) {
            Optional<Path> archiveFile = Files.list(archivePath)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().contains(account))
                    .findFirst();

            if (archiveFile.isPresent()) {
                archiveBlobUrl = blobStorageService.uploadFileByMessage(
                        archiveFile.get().toFile(), "archive", msg);
                archiveStatus = "SUCCESS";

                SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, archiveEntry);
                archiveEntry.setOutputType("ARCHIVE");
                archiveEntry.setBlobUrl(archiveBlobUrl);
                archiveEntry.setStatus(archiveStatus);
                archiveEntry.setOverallStatus(archiveStatus);
                finalList.add(archiveEntry);
            }
        }

        // Process EMAIL, MOBSTAT, PRINT
        for (String folder : deliveryFolders) {
            String outputMethod = folderToOutputMethod.get(folder);
            Path methodPath = jobDir.resolve(folder);

            String blobUrl = "";
            String deliveryStatus;
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

            if (fileFound) {
                deliveryStatus = "SUCCESS";
            } else if (customerErrors.containsKey(outputMethod) &&
                    "FAILED".equalsIgnoreCase(customerErrors.get(outputMethod))) {
                deliveryStatus = "FAILED";
            } else if (!fileFound && (!customerErrors.containsKey(outputMethod) || customerErrors.get(outputMethod).isBlank())) {
                deliveryStatus = ""; // ⬅️ Replaced "NOT-FOUND" with ""
            } else {
                deliveryStatus = "FAILED"; // fallback safety
            }

            String overallStatus = deliveryStatus;

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
