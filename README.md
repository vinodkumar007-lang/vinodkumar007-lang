private static Metadata buildMetadata(List<ProcessedFileEntry> processedFileEntries, String batchId, String fileName, KafkaMessage kafkaMessage) {
    Metadata metadata = new Metadata();

    // ✅ Count all rows that have at least one file (include same account with different filenames)
    long totalCustomersProcessed = processedFileEntries.stream()
            .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl())
                       || isNonEmpty(pf.getEmailBlobUrlPdf())
                       || isNonEmpty(pf.getEmailBlobUrlHtml())
                       || isNonEmpty(pf.getEmailBlobUrlText())
                       || isNonEmpty(pf.getPrintFileUrl())
                       || isNonEmpty(pf.getPdfMobstatFileUrl()))
            .count();
    metadata.setTotalCustomersProcessed((int) totalCustomersProcessed);

    // Determine overall status
    Set<String> statuses = processedFileEntries.stream()
            .map(ProcessedFileEntry::getOverallStatus)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

    String overallStatus;
    if (statuses.size() == 1) overallStatus = statuses.iterator().next();
    else if (statuses.contains("SUCCESS") && statuses.contains("FAILED")) overallStatus = "PARTIAL";
    else if (statuses.contains("PARTIAL") || statuses.size() > 1) overallStatus = "PARTIAL";
    else overallStatus = "FAILED";

    // Original customerCount from Kafka message
    int customerCount = kafkaMessage.getBatchFiles().stream()
            .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
            .mapToInt(BatchFile::getCustomerCount)
            .sum();
    metadata.setCustomerCount(customerCount);

    metadata.setProcessingStatus(overallStatus);
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription(overallStatus.toLowerCase());

    logger.info("[METADATA] batchId={}, fileName={}, totalCustomersProcessed={}, customerCount={}, status={}",
            batchId, fileName, totalCustomersProcessed, customerCount, overallStatus);

    return metadata;
}
