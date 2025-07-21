private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    // Define expected methods per customer
    List<String> expectedMethods = Arrays.asList("EMAIL", "ARCHIVE", "PRINT", "MOBSTAT");

    // Group by customerId + accountNumber
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        List<SummaryProcessedFile> records = entry.getValue();

        Map<String, SummaryProcessedFile> methodMap = new HashMap<>();
        for (SummaryProcessedFile file : records) {
            methodMap.put(file.getOutputType(), file);
        }

        List<SummaryProcessedFile> completeRecordList = new ArrayList<>();
        boolean hasFailed = false;
        boolean hasNotFound = false;

        for (String method : expectedMethods) {
            SummaryProcessedFile record = methodMap.get(method);
            if (record != null) {
                String status = record.getStatus();
                if ("FAILED".equalsIgnoreCase(status)) hasFailed = true;
                if ("NOT_FOUND".equalsIgnoreCase(status)) hasNotFound = true;
                completeRecordList.add(record);
            } else {
                // Method not found, mark explicitly as NOT_FOUND
                SummaryProcessedFile missing = new SummaryProcessedFile();
                missing.setCustomerId(customerId);
                missing.setAccountNumber(accountNumber);
                missing.setOutputType(method);
                missing.setBlobUrl(null);
                missing.setStatus("NOT_FOUND");
                completeRecordList.add(missing);
                hasNotFound = true;
            }
        }

        String overallStatus;
        if (hasFailed) {
            overallStatus = "FAILED";
        } else if (hasNotFound) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "SUCCESS";
        }

        ProcessedFileEntry finalEntry = new ProcessedFileEntry();
        finalEntry.setCustomerId(customerId);
        finalEntry.setAccountNumber(accountNumber);
        finalEntry.setOutputTypes(completeRecordList);
        finalEntry.setOverAllStatusCode(overallStatus);

        finalList.add(finalEntry);
    }

    return finalList;
}
