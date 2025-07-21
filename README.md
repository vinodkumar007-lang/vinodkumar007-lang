public static List<SummaryProcessedFile> buildDetailedProcessedFiles(
        List<SummaryProcessedFile> originalList,
        Map<String, Map<String, String>> errorMap) {

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = originalList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    List<String> methods = Arrays.asList("EMAIL", "ARCHIVE", "PRINT", "MOBSTAT");

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] keys = entry.getKey().split("::");
        String custId = keys[0];
        String accNum = keys[1];

        Map<String, SummaryProcessedFile> methodMap = entry.getValue().stream()
                .collect(Collectors.toMap(SummaryProcessedFile::getOutputType, f -> f));

        for (String method : methods) {
            if (methodMap.containsKey(method)) {
                // Found — add as-is
                finalList.add(methodMap.get(method));
            } else {
                // Missing — determine status using errorMap
                SummaryProcessedFile missing = new SummaryProcessedFile();
                missing.setCustomerId(custId);
                missing.setAccountNumber(accNum);
                missing.setOutputType(method);

                if (isMethodFailedInErrorMap(errorMap, custId, accNum, method)) {
                    missing.setStatus("FAILED");
                } else {
                    missing.setStatus("NOT_FOUND");
                }

                finalList.add(missing);
            }
        }
    }

    return finalList;
}

======
private static boolean isMethodFailedInErrorMap(Map<String, Map<String, String>> errorMap,
                                                String customerId,
                                                String accountNumber,
                                                String method) {
    String key = customerId + "::" + accountNumber;
    Map<String, String> methodMap = errorMap.get(key);
    if (methodMap == null) return false;
    return methodMap.containsKey(method);
}

==========
private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        List<SummaryProcessedFile> records = entry.getValue();

        List<String> statuses = records.stream()
                .map(SummaryProcessedFile::getStatus)
                .filter(Objects::nonNull)
                .map(String::toUpperCase)
                .collect(Collectors.toList());

        String overallStatus;

        if (statuses.stream().allMatch(s -> s.equals("SUCCESS"))) {
            overallStatus = "SUCCESS";
        } else if (statuses.stream().allMatch(s -> s.equals("FAILED"))) {
            overallStatus = "FAILED";
        } else {
            overallStatus = "PARTIAL";
        }

        ProcessedFileEntry entryObj = new ProcessedFileEntry();
        entryObj.setCustomerId(customerId);
        entryObj.setAccountNumber(accountNumber);
        entryObj.setOutputTypes(records); // includes EMAIL/ARCHIVE/PRINT/MOBSTAT with status
        entryObj.setOverallStatus(overallStatus);

        finalList.add(entryObj);
    }

    return finalList;
}
