private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    for (SummaryProcessedFile customer : customerList) {
        String customerId = customer.getCustomerId();
        String account = customer.getAccountNumber();

        Map<String, SummaryProcessedFile> combinedMap = new HashMap<>();

        for (String deliveryFolder : folders) {
            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);

            // Delivery file search
            Optional<Path> deliveryMatch = FileUtil.findFirstMatchingFile(jobDir.resolve(deliveryFolder), account);
            if (deliveryMatch.isPresent()) {
                Path deliveryFile = deliveryMatch.get();
                String deliveryBlobUrl = blobStorageService.uploadFileAndReturnLocation(
                        deliveryFile, msg.getSourceSystem(), msg.getConsumerReference(), msg.getProcessReference(), msg.getTimestamp());

                entry.setBlobUrl(deliveryBlobUrl);
                entry.setOutputMethod(folderToOutputMethod.get(deliveryFolder));
                entry.setStatus("SUCCESS");
            } else {
                entry.setOutputMethod(folderToOutputMethod.get(deliveryFolder));
                entry.setBlobUrl(null);
                entry.setStatus("FAILED");
            }

            // Archive file search (same archive file reused for all delivery types)
            Optional<Path> archiveMatch = FileUtil.findFirstMatchingFile(jobDir.resolve("archive"), account);
            if (archiveMatch.isPresent()) {
                Path archiveFile = archiveMatch.get();
                String archiveBlobUrl = blobStorageService.uploadFileAndReturnLocation(
                        archiveFile, msg.getSourceSystem(), msg.getConsumerReference(), msg.getProcessReference(), msg.getTimestamp());

                entry.setArchiveBlobUrl(archiveBlobUrl);
                entry.setArchiveOutputMethod("ARCHIVE");
                entry.setArchiveStatus("SUCCESS");
            } else {
                entry.setArchiveBlobUrl(null);
                entry.setArchiveOutputMethod("ARCHIVE");
                entry.setArchiveStatus("FAILED");
            }

            // Set overall status
            if ("SUCCESS".equalsIgnoreCase(entry.getStatus()) && "SUCCESS".equalsIgnoreCase(entry.getArchiveStatus())) {
                entry.setOverallStatus("SUCCESS");
            } else {
                entry.setOverallStatus("FAILED");
            }

            // Add to final list
            finalList.add(entry);
        }

        // Also add any errorMap records if available
        if (errorMap.containsKey(customerId + "::" + account)) {
            Map<String, String> errors = errorMap.get(customerId + "::" + account);
            for (Map.Entry<String, String> e : errors.entrySet()) {
                SummaryProcessedFile errorEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, errorEntry);
                errorEntry.setOutputMethod(e.getKey());
                errorEntry.setStatus("FAILED");
                errorEntry.setOverallStatus("FAILED");
                errorEntry.setRemarks(e.getValue());
                finalList.add(errorEntry);
            }
        }
    }

    return finalList;
}

private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    Map<String, ProcessedFileEntry> entryMap = new LinkedHashMap<>();

    Map<String, List<SummaryProcessedFile>> groupedFiles = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(
                    f -> f.getCustomerId() + "::" + f.getAccountNumber(),
                    LinkedHashMap::new,
                    Collectors.toList()
            ));

    for (Map.Entry<String, List<SummaryProcessedFile>> group : groupedFiles.entrySet()) {
        String key = group.getKey();
        List<SummaryProcessedFile> files = group.getValue();

        ProcessedFileEntry entry = new ProcessedFileEntry();
        String[] parts = key.split("::");
        entry.setCustomerId(parts[0]);
        entry.setAccountNumber(parts[1]);

        List<String> statuses = new ArrayList<>();
        List<String> failureReasons = new ArrayList<>();

        for (SummaryProcessedFile file : files) {
            String method = file.getOutputMethod();
            if (method == null) continue;

            String status = file.getStatus() != null ? file.getStatus() : "UNKNOWN";
            statuses.add(status);

            String blobURL = file.getBlobURL();
            String archiveUrl = file.getArchiveBlobUrl();
            String archiveStatus = file.getArchiveStatus();
            String reason = file.getStatusDescription();

            switch (method.toUpperCase()) {
                case "EMAIL" -> {
                    if (blobURL != null) entry.setPdfEmailFileUrl(blobURL);
                    if (status != null) entry.setPdfEmailFileUrlStatus(status);
                    if (archiveUrl != null) entry.setPdfArchiveFileUrl(archiveUrl);
                    if (archiveStatus != null) entry.setPdfArchiveFileUrlStatus(archiveStatus);
                    if ("FAILED".equalsIgnoreCase(status) && reason != null) failureReasons.add(reason);
                }
                case "MOBSTAT" -> {
                    if (blobURL != null) entry.setPdfMobstatFileUrl(blobURL);
                    if (status != null) entry.setPdfMobstatFileUrlStatus(status);
                    if (archiveUrl != null) entry.setPdfMobstatArchiveFileUrl(archiveUrl);
                    if (archiveStatus != null) entry.setPdfMobstatArchiveFileUrlStatus(archiveStatus);
                    if ("FAILED".equalsIgnoreCase(status) && reason != null) failureReasons.add(reason);
                }
                case "PRINT" -> {
                    if (blobURL != null) entry.setPrintFileUrl(blobURL);
                    if (status != null) entry.setPrintFileUrlStatus(status);
                    if (archiveUrl != null) entry.setPrintArchiveFileUrl(archiveUrl);
                    if (archiveStatus != null) entry.setPrintArchiveFileUrlStatus(archiveStatus);
                    if ("FAILED".equalsIgnoreCase(status) && reason != null) failureReasons.add(reason);
                }
            }
        }

        // Set overall status
        boolean allSuccess = statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s));
        boolean noneSuccess = statuses.stream().noneMatch(s -> "SUCCESS".equalsIgnoreCase(s));
        String overallStatus = allSuccess ? "SUCCESS" : noneSuccess ? "FAILURE" : "PARTIAL";
        entry.setOverAllStatusCode(overallStatus);

        // Combine all failure reasons if needed
        if (!failureReasons.isEmpty()) {
            entry.setReason(String.join(" | ", failureReasons));
        }

        entryMap.put(key, entry);
    }

    return new ArrayList<>(entryMap.values());
}
