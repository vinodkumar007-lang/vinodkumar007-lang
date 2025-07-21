private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<String> folders = List.of("email", "archive", "mobstat", "print");
        Map<String, String> folderToOutputMethod = Map.of(
                "email", "EMAIL",
                "archive", "ARCHIVE",
                "mobstat", "MOBSTAT",
                "print", "PRINT"
        );

        List<SummaryProcessedFile> finalList = new ArrayList<>();

        for (SummaryProcessedFile customer : customerList) {
            String account = customer.getAccountNumber();
            String customerId = customer.getCustomerId();
            boolean hasAtLeastOneSuccess = false;
            Map<String, Boolean> methodAdded = new HashMap<>();

            for (String folder : folders) {
                methodAdded.put(folder, false); // to prevent duplicates
                Path folderPath = jobDir.resolve(folder);
                if (!Files.exists(folderPath)) continue;

                Optional<Path> match = Files.list(folderPath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (match.isPresent()) {
                    Path filePath = match.get();
                    File file = filePath.toFile();
                    String blobUrl = blobStorageService.uploadFile(file, folder, msg);

                    SummaryProcessedFile successEntry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, successEntry);
                    successEntry.setOutputMethod(folderToOutputMethod.get(folder));
                    successEntry.setStatus("SUCCESS");
                    successEntry.setBlobURL(blobUrl);
                    finalList.add(successEntry);

                    hasAtLeastOneSuccess = true;
                    methodAdded.put(folder, true);
                }
            }

            // Add failed entries for folders that exist but file is missing
            for (String folder : folders) {
                if (methodAdded.get(folder)) continue; // Already added

                Path folderPath = jobDir.resolve(folder);
                if (Files.exists(folderPath)) {
                    // Folder exists, but file not found
                    SummaryProcessedFile failedEntry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, failedEntry);
                    failedEntry.setOutputMethod(folderToOutputMethod.get(folder));
                    failedEntry.setStatus("FAILED");
                    failedEntry.setStatusDescription("File not found for method: " + folder);
                    failedEntry.setReason("File not found in " + folder + " folder");
                    failedEntry.setBlobURL(null);
                    finalList.add(failedEntry);
                }
            }

            // Add error report failure if present
            if (errorMap.containsKey(account)) {
                Map<String, String> errData = errorMap.get(account);
                SummaryProcessedFile errorEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, errorEntry);
                errorEntry.setOutputMethod("ERROR_REPORT");
                errorEntry.setStatus("FAILED");
                errorEntry.setStatusDescription("Marked as failed from ErrorReport");
                errorEntry.setReason(errData.get("reason"));
                errorEntry.setBlobURL(errData.get("blobURL"));
                finalList.add(errorEntry);
            }
        }

        return finalList;
    }

        public static SummaryPayload buildPayload(
            KafkaMessage kafkaMessage,
            List<SummaryProcessedFile> processedList,
            String summaryBlobUrl,
            String fileName,
            String batchId,
            String timestamp
    ) {
        SummaryPayload payload = new SummaryPayload();
        payload.setBatchID(batchId);
        payload.setFileName(fileName);
        payload.setTimestamp(timestamp);
        payload.setSummaryFileURL(summaryBlobUrl);

        // HEADER
        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        // ✅ Final Processed Entries
        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
        payload.setProcessedFileList(processedFileEntries);

        // ✅ Dynamically count total non-null URLs (files actually added to summary)
        int totalFileUrls = processedFileEntries.stream()
                .mapToInt(entry -> {
                    int count = 0;
                    if (entry.getPdfEmailFileUrl() != null && !entry.getPdfEmailFileUrl().isBlank()) count++;
                    if (entry.getPdfArchiveFileUrl() != null && !entry.getPdfArchiveFileUrl().isBlank()) count++;
                    if (entry.getPdfMobstatFileUrl() != null && !entry.getPdfMobstatFileUrl().isBlank()) count++;
                    if (entry.getPrintFileUrl() != null && !entry.getPrintFileUrl().isBlank()) count++;
                    return count;
                })
                .sum();

        // PAYLOAD BLOCK
        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
        payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
        payloadInfo.setEventID(kafkaMessage.getEventID());
        payloadInfo.setEventType(kafkaMessage.getEventType());
        payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
        payloadInfo.setFileCount(totalFileUrls);
        payload.setPayload(payloadInfo);

        // METADATA
        Metadata metadata = new Metadata();
        metadata.setTotalCustomersProcessed((int) processedFileEntries.stream()
                .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
                .distinct()
                .count());

        // ✅ Determine overall status (Success / Partial / Failure)
        long total = processedFileEntries.size();
        long success = processedFileEntries.stream()
                .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverAllStatusCode()))
                .count();
        long failure = processedFileEntries.stream()
                .filter(entry -> "FAILURE".equalsIgnoreCase(entry.getOverAllStatusCode()))
                .count();

        String overallStatus;
        if (success == total) {
            overallStatus = "SUCCESS";
        } else if (failure == total) {
            overallStatus = "FAILURE";
        } else {
            overallStatus = "PARTIAL";
        }

        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        return payload;
    }

    private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
        Map<String, ProcessedFileEntry> entryMap = new LinkedHashMap<>();
        Map<String, List<String>> statusTracker = new HashMap<>();

        for (SummaryProcessedFile file : processedList) {
            String customerId = file.getCustomerId();
            String accountNumber = file.getAccountNumber();
            String blobURL = file.getBlobURL();
            String status = file.getStatus() != null ? file.getStatus() : "UNKNOWN";
            String outputMethod = file.getOutputMethod() != null ? file.getOutputMethod().toLowerCase() : "";
            String statusDesc = file.getStatusDescription();

            if (customerId == null || accountNumber == null) continue;
            if ("NOT_FOUND".equalsIgnoreCase(status)) continue;  // optional: keep it skipped

            String key = customerId + "::" + accountNumber;

            ProcessedFileEntry entry = entryMap.computeIfAbsent(key, k -> {
                ProcessedFileEntry e = new ProcessedFileEntry();
                e.setCustomerId(customerId);
                e.setAccountNumber(accountNumber);
                return e;
            });

            // Track all statuses for final overall status
            statusTracker.computeIfAbsent(key, k -> new ArrayList<>()).add(status);

            // Assign file URLs + status for each method
            switch (outputMethod) {
                case "email" -> {
                    entry.setPdfEmailFileUrl(blobURL);
                    entry.setPdfEmailFileUrlStatus(status);
                    if ("FAILED".equalsIgnoreCase(status)) entry.setReason(statusDesc);
                }
                case "archive" -> {
                    entry.setPdfArchiveFileUrl(blobURL);
                    entry.setPdfArchiveFileUrlStatus(status);
                    if ("FAILED".equalsIgnoreCase(status)) entry.setReason(statusDesc);
                }
                case "mobstat" -> {
                    entry.setPdfMobstatFileUrl(blobURL);
                    entry.setPdfMobstatFileUrlStatus(status);
                    if ("FAILED".equalsIgnoreCase(status)) entry.setReason(statusDesc);
                }
                case "print" -> {
                    entry.setPrintFileUrl(blobURL);
                    entry.setPrintFileUrlStatus(status);
                    if ("FAILED".equalsIgnoreCase(status)) entry.setReason(statusDesc);
                }
            }
        }

        // Set overall status for each grouped customer-account
        for (Map.Entry<String, ProcessedFileEntry> groupedEntry : entryMap.entrySet()) {
            List<String> statuses = statusTracker.getOrDefault(groupedEntry.getKey(), List.of());

            String overallStatus;
            boolean allSuccess = statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s));
            boolean allFailed = statuses.stream().allMatch(s -> "FAILED".equalsIgnoreCase(s));

            if (allSuccess) {
                overallStatus = "SUCCESS";
            } else if (allFailed) {
                overallStatus = "FAILURE";
            } else {
                overallStatus = "PARTIAL";
            }

            groupedEntry.getValue().setOverAllStatusCode(overallStatus);
        }

        return new ArrayList<>(entryMap.values());
    }
