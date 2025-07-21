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
            entryObj.setOverAllStatusCode(overallStatus);

            finalList.add(entryObj);
        }

        return finalList;
    }
