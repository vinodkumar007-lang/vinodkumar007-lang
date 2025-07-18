private static List<CustomerSummary> buildCustomerSummaries(List<SummaryProcessedFile> processedFiles) {
    List<CustomerSummary> resultList = new ArrayList<>();

    Map<String, Map<String, List<SummaryProcessedFile>>> grouped = new HashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        if (file.getCustomerId() == null || file.getAccountNumber() == null) continue;

        grouped
            .computeIfAbsent(file.getCustomerId(), k -> new HashMap<>())
            .computeIfAbsent(file.getAccountNumber(), k -> new ArrayList<>())
            .add(file);
    }

    for (Map.Entry<String, Map<String, List<SummaryProcessedFile>>> customerEntry : grouped.entrySet()) {
        String customerId = customerEntry.getKey();
        Map<String, List<SummaryProcessedFile>> accountMap = customerEntry.getValue();

        CustomerSummary customerSummary = new CustomerSummary();
        customerSummary.setCustomerId(customerId);

        int totalAccounts = 0;
        int totalSuccess = 0;
        int totalFailures = 0;

        for (Map.Entry<String, List<SummaryProcessedFile>> accountEntry : accountMap.entrySet()) {
            String accountNumber = accountEntry.getKey();
            List<SummaryProcessedFile> files = accountEntry.getValue();

            AccountSummary acc = new AccountSummary();
            acc.setAccountNumber(accountNumber);

            for (SummaryProcessedFile file : files) {
                String method = file.getOutputMethod() != null ? file.getOutputMethod().toUpperCase() : "";
                String status = file.getStatus();
                String url = file.getBlobURL();

                switch (method) {
                    case "EMAIL" -> {
                        acc.setPdfEmailStatus(status);
                        acc.setPdfEmailBlobUrl(url);
                    }
                    case "ARCHIVE" -> {
                        acc.setPdfArchiveStatus(status);
                        acc.setPdfArchiveBlobUrl(url);
                    }
                    case "MOBSTAT" -> {
                        acc.setPdfMobstatStatus(status);
                        acc.setPdfMobstatBlobUrl(url);
                    }
                    case "PRINT" -> {
                        acc.setPrintStatus(status);
                        acc.setPrintBlobUrl(url);
                    }
                    default -> logger.warn("❗ Unrecognized output method: {}", method);
                }

                if ("SUCCESS".equalsIgnoreCase(status)) totalSuccess++;
                else totalFailures++;
            }

            customerSummary.getAccounts().add(acc);
            totalAccounts++;
        }

        customerSummary.setTotalAccounts(totalAccounts);
        customerSummary.setTotalSuccess(totalSuccess);
        customerSummary.setTotalFailure(totalFailures);

        resultList.add(customerSummary);
    }

    return resultList;
}
