public static SummaryPayload buildPayload(KafkaMessage message,
                                              List<SummaryProcessedFile> processedFiles,
                                              int pagesProcessed,
                                              List<PrintFile> printFiles,
                                              String mobstatTriggerPath,
                                              int customersProcessed) {

        SummaryPayload payload = new SummaryPayload();

        payload.setBatchID(message.getBatchId());
        payload.setFileName(message.getBatchId() + ".csv");
        payload.setMobstatTriggerFile(mobstatTriggerPath);

        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());
        header.setTimestamp(Instant.now().toString());
        payload.setHeader(header);

        String overallStatus = "Completed";
        if (processedFiles != null && !processedFiles.isEmpty()) {
            boolean allFailed = processedFiles.stream().allMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()));
            boolean anyFailed = processedFiles.stream().anyMatch(f ->
                    "FAILURE".equalsIgnoreCase(f.getStatusCode()) || "PARTIAL".equalsIgnoreCase(f.getStatusCode()));
            if (allFailed) overallStatus = "Failure";
            else if (anyFailed) overallStatus = "Partial";
        }

        Metadata metadata = new Metadata();
        metadata.setTotalFilesProcessed(customersProcessed);
        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        payload.setMetadata(metadata);

        Payload payloadDetails = new Payload();
        payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
        payloadDetails.setRunPriority(message.getRunPriority());
        payloadDetails.setEventID(message.getEventID());
        payloadDetails.setEventType(message.getEventType());
        payloadDetails.setRestartKey(message.getRestartKey());
        payloadDetails.setFileCount(pagesProcessed);
        payload.setPayload(payloadDetails);

        List<CustomerSummary> customerSummaries = buildCustomerSummaries(processedFiles);
        payload.setCustomerSummaries(customerSummaries);

        // Optional: include printFiles if needed
        payload.setPrintFiles(printFiles);

        return payload;
    }

    private static List<CustomerSummary> buildCustomerSummaries(List<SummaryProcessedFile> processedFiles) {
        List<CustomerSummary> resultList = new ArrayList<>();

        Map<String, Map<String, List<SummaryProcessedFile>>> grouped = new HashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            if (file.getCustomerId() == null || file.getAccountNumber() == null) continue;

            grouped
                    .computeIfAbsent(file.getCustomerId(), k -> new HashMap<>())
                    .computeIfAbsent(file.getAccountNumber(), k -> new ArrayList<>())
                    .add(file);
        }

        for (Map.Entry<String, Map<String, List<SummaryProcessedFile>>> customerEntry : grouped.entrySet()) {
            String customerId = customerEntry.getKey();
            Map<String, List<SummaryProcessedFile>> accountMap = customerEntry.getValue();

            CustomerSummary customerSummary = new CustomerSummary();
            customerSummary.setCustomerId(customerId);

            int totalAccounts = 0;
            int totalSuccess = 0;
            int totalFailures = 0;

            for (Map.Entry<String, List<SummaryProcessedFile>> accountEntry : accountMap.entrySet()) {
                String accountNumber = accountEntry.getKey();
                List<SummaryProcessedFile> files = accountEntry.getValue();

                AccountSummary acc = new AccountSummary();
                acc.setAccountNumber(accountNumber);

                for (SummaryProcessedFile file : files) {
                    String method = file.getOutputMethod(); // ✅ fixed
                    String status = file.getStatus();
                    String url = file.getBlobURL();

                    if ("EMAIL".equalsIgnoreCase(method)) {
                        acc.setPdfEmailStatus(status);
                        acc.setPdfEmailBlobUrl(url);
                    } else if ("ARCHIVE".equalsIgnoreCase(method)) {
                        acc.setPdfArchiveStatus(status);
                        acc.setPdfArchiveBlobUrl(url);
                    } else if ("MOBSTAT".equalsIgnoreCase(method)) {
                        acc.setPdfMobstatStatus(status);
                        acc.setPdfMobstatBlobUrl(url);
                    } else if ("PRINT".equalsIgnoreCase(method)) {
                        acc.setPrintStatus(status);
                        acc.setPrintBlobUrl(url);
                    } else {
                        logger.warn("❗ Unrecognized output method: {}", method);
                    }

                    if ("success".equalsIgnoreCase(status)) totalSuccess++;
                    else totalFailures++;
                }

                customerSummary.getAccounts().add(acc);
                totalAccounts++;
            }

            customerSummary.setTotalAccounts(totalAccounts);
            customerSummary.setTotalSuccess(totalSuccess);
            customerSummary.setTotalFailure(totalFailures);

            resultList.add(customerSummary);
        }

        return resultList;
    }
