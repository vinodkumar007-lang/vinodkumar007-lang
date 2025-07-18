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

        // Header section from Kafka
        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        //header.setConsumerReference(kafkaMessage.getConsumerReference());
        //header.setProcessReference(kafkaMessage.getProcessReference());
        payload.setHeader(header);

        // Metadata grouping processed files by customerId
        Metadata metadata = new Metadata();
        payload.setCustomerSummaries(groupProcessedFilesByCustomerId(processedList));
        payload.setMetadata(metadata);

        // Flat processed list also set
        //payload.setProcessedList(processedList);

        return payload;
    }

    private static List<CustomerSummary> groupProcessedFilesByCustomerId(List<SummaryProcessedFile> processedList) {
        Map<String, CustomerSummary> customerMap = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedList) {
            String customerId = file.getCustomerId();
            if (customerId == null || customerId.isBlank()) continue;

            CustomerSummary summary = customerMap.computeIfAbsent(customerId, id -> {
                CustomerSummary cs = new CustomerSummary();
                cs.setCustomerId(id);
                cs.setFiles(new ArrayList<>());
                return cs;
            });

            summary.getSummaryProcessedFileList().add(file);
        }

        return new ArrayList<>(customerMap.values());
    }
