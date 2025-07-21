private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    // Group by customerId::accountNumber
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        List<SummaryProcessedFile> files = entry.getValue();

        String customerId = files.get(0).getCustomerId();
        String accountNumber = files.get(0).getAccountNumber();

        boolean hasFailed = false;
        boolean hasNotFound = false;
        boolean allSuccess = true;

        for (SummaryProcessedFile file : files) {
            if ("FAILED".equalsIgnoreCase(file.getStatus())) {
                hasFailed = true;
                allSuccess = false;
            } else if ("NOT_FOUND".equalsIgnoreCase(file.getStatus())) {
                hasNotFound = true;
                allSuccess = false;
            }
        }

        String overallStatus;
        if (allSuccess) {
            overallStatus = "SUCCESS";
        } else if (hasFailed) {
            overallStatus = "FAILED";
        } else {
            overallStatus = "PARTIAL";
        }

        // Set overallStatus to all entries for this customer
        files.forEach(f -> f.setOverallStatusCode(overallStatus));

        // Create the ProcessedFileEntry
        ProcessedFileEntry entryObj = new ProcessedFileEntry();
        entryObj.setCustomerId(customerId);
        entryObj.setAccountNumber(accountNumber);
        entryObj.setOverallStatus(overallStatus);
        entryObj.setFiles(files);  // attach full list of method-specific files

        finalList.add(entryObj);
    }

    return finalList;
}

private static List<SummaryProcessedFile> buildDetailedProcessedFiles(
        List<String> outputMethods,
        String customerId,
        String accountNumber,
        String batchId,
        Map<String, String> customerFileUrls,
        Map<String, Map<String, String>> errorMap) {

    List<SummaryProcessedFile> detailedList = new ArrayList<>();
    Map<String, String> methodErrors = errorMap.getOrDefault(customerId + "::" + accountNumber, new HashMap<>());

    for (String method : outputMethods) {
        SummaryProcessedFile file = new SummaryProcessedFile();
        file.setCustomerId(customerId);
        file.setAccountNumber(accountNumber);
        file.setBatchId(batchId);
        file.setOutputType(method);

        String url = customerFileUrls.getOrDefault(method, null);
        file.setBlobURL(url);

        if (url != null) {
            file.setStatus("SUCCESS");
        } else if (methodErrors.containsKey(method)) {
            file.setStatus("FAILED");
        } else {
            file.setStatus("NOT_FOUND");
        }

        // 🔴 Don't set overallStatusCode here
        detailedList.add(file);
    }

    return detailedList;
}
