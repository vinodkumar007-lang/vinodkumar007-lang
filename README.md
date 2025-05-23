public Map<String, Object> listen() {
    Consumer<String, String> consumer = consumerFactory.createConsumer();
    try {
        List<TopicPartition> partitions = new ArrayList<>();
        consumer.partitionsFor(inputTopic).forEach(partitionInfo ->
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        );

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        List<String> recentMessages = new ArrayList<>();
        int emptyPollCount = 0;
        long threeDaysAgo = System.currentTimeMillis() - Duration.ofDays(3).toMillis();

        while (emptyPollCount < 3) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                emptyPollCount++;
            } else {
                emptyPollCount = 0;
                for (ConsumerRecord<String, String> record : records) {
                    if (record.timestamp() >= threeDaysAgo) {
                        logger.info("Received message (offset={}): {}", record.offset(), record.value());
                        recentMessages.add(record.value());
                    } else {
                        logger.debug("Skipping old message (timestamp={}): {}", record.timestamp(), record.value());
                    }
                }
            }
        }

        if (recentMessages.isEmpty()) {
            return generateErrorResponse("204", "No recent messages found in Kafka topic.");
        }

        // Process each message individually and append/update summary.json
        List<SummaryPayload> processedPayloads = new ArrayList<>();
        for (String message : recentMessages) {
            SummaryPayload summaryPayload = processSingleMessage(message);
            appendSummaryToFile(summaryPayload);
            processedPayloads.add(summaryPayload);
        }

        // Combine all SummaryPayloads into one final summary
        SummaryPayload finalSummary = mergeSummaryPayloads(processedPayloads);

        // Serialize final summary to JSON string and send to output Kafka topic
        String finalSummaryJson = objectMapper.writeValueAsString(finalSummary);
        kafkaTemplate.send(outputTopic, finalSummaryJson);
        logger.info("Final combined summary sent to topic: {}", outputTopic);

        // Return the final summary as Map (for your REST response)
        return objectMapper.convertValue(finalSummary, Map.class);

    } catch (Exception e) {
        logger.error("Error during Kafka message processing", e);
        return generateErrorResponse("500", "Internal Server Error while processing messages.");
    } finally {
        consumer.close();
    }
}

/**
 * Merge multiple SummaryPayload objects into one combined SummaryPayload.
 * Adjust merging logic as needed.
 */
private SummaryPayload mergeSummaryPayloads(List<SummaryPayload> payloads) {
    if (payloads.isEmpty()) {
        return new SummaryPayload();
    }
    SummaryPayload merged = new SummaryPayload();

    // Merge Headers - example: take first or combine fields intelligently
    HeaderInfo mergedHeader = new HeaderInfo();
    // You can customize this; here we take first payload's header for simplicity
    mergedHeader = payloads.get(0).getHeader();
    merged.setHeader(mergedHeader);

    // Merge PayloadInfo - you can customize merging strategy here
    PayloadInfo mergedPayloadInfo = new PayloadInfo();
    List<Object> combinedPrintFiles = new ArrayList<>();
    for (SummaryPayload sp : payloads) {
        if (sp.getPayload() != null && sp.getPayload().getPrintFiles() != null) {
            combinedPrintFiles.addAll(sp.getPayload().getPrintFiles());
        }
    }
    mergedPayloadInfo.setPrintFiles(combinedPrintFiles);
    merged.setPayload(mergedPayloadInfo);

    // Merge MetadataInfo (customer summaries)
    MetaDataInfo mergedMeta = new MetaDataInfo();
    List<CustomerSummary> combinedCustomers = new ArrayList<>();
    for (SummaryPayload sp : payloads) {
        if (sp.getMetadata() != null && sp.getMetadata().getCustomerSummaries() != null) {
            combinedCustomers.addAll(sp.getMetadata().getCustomerSummaries());
        }
    }
    mergedMeta.setCustomerSummaries(combinedCustomers);
    merged.setMetadata(mergedMeta);

    return merged;
}
