List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
logger.info("\uD83D\uDCCA Total customerSummaries parsed: {}", customerSummaries.size());

List<SummaryProcessedFile> customerList = customerSummaries.stream()
        .map(cs -> {
            SummaryProcessedFile spf = new SummaryProcessedFile();
            spf.setAccountNumber(cs.getAccountNumber());
            spf.setCustomerId(cs.getCisNumber());
            return spf;
        })
        .collect(Collectors.toList());

Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

List<SummaryProcessedFile> processedFiles =
        buildDetailedProcessedFiles(jobDir, customerList, errorMap, message);
logger.info("\uD83D\uDCE6 Processed {} customer records", processedFiles.size());
