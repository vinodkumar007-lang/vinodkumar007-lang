// Populate customerMap from parsed summaries
Map<String, SummaryProcessedFile> customerMap = new HashMap<>();
for (CustomerSummary cs : customerSummaries) {
    SummaryProcessedFile pf = new SummaryProcessedFile();
    pf.setAccountNumber(cs.getAccountNumber());
    pf.setCisNumber(cs.getCisNumber());
    pf.setCustomerId(cs.getCustomerId());
    customerMap.put(cs.getAccountNumber(), pf);
}

// Build processedFiles[]
List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(jobDir, customerMap, errorMap, message);
