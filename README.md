List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
logger.info("\uD83D\uDCCA Total customerSummaries parsed: {}", customerSummaries.size());

Map<String, SummaryProcessedFile> customerMap = customerSummaries.stream()
        .filter(cs -> cs.getAccountNumber() != null && !cs.getAccountNumber().isBlank())
        .collect(Collectors.toMap(
            CustomerSummary::getAccountNumber,
            cs -> {
                SummaryProcessedFile spf = new SummaryProcessedFile();
                spf.setAccountNumber(cs.getAccountNumber());
                spf.setCustomerId(cs.getCisNumber());
                return spf;
            },
            (existing, incoming) -> mergeFiles(existing, incoming) // ✅ merge duplicates safely
        ));

Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

List<SummaryProcessedFile> processedFiles =
        buildDetailedProcessedFiles(jobDir, customerMap, errorMap, message);
logger.info("\uD83D\uDCE6 Processed {} customer records", processedFiles.size());
