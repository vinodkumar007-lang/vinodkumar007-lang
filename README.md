private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList, Map<String, String> errorMap) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    // Group by customerId::accountNumber
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];
        List<SummaryProcessedFile> list = entry.getValue();

        ProcessedFileEntry pfEntry = new ProcessedFileEntry();
        pfEntry.setCustomerId(customerId);
        pfEntry.setAccountNumber(accountNumber);

        for (SummaryProcessedFile file : list) {
            String method = file.getOutputType();
            String status = file.getStatus();
            String url = file.getBlobUrl();

            switch (method.toUpperCase()) {
                case "EMAIL":
                    pfEntry.setEmailblobUrl(url);
                    pfEntry.setEmailstatus(status);
                    break;
                case "ARCHIVE":
                    pfEntry.setArchiveblobUrl(url);
                    pfEntry.setArchivestatus(status);
                    break;
                case "MOBSTAT":
                    pfEntry.setMobstatblobUrl(url);
                    pfEntry.setMobstatstatus(status);
                    break;
                case "PRINT":
                    pfEntry.setPrintblobUrl(url);
                    pfEntry.setPrintstatus(status);
                    break;
            }
        }

        // Determine overallStatus for combinations: EMAIL+ARCHIVE, MOBSTAT+ARCHIVE, PRINT+ARCHIVE
        pfEntry.setOverallStatus(determineOverallStatus(pfEntry, errorMap));
        finalList.add(pfEntry);
    }

    return finalList;
}

private static String determineOverallStatus(ProcessedFileEntry pf, Map<String, String> errorMap) {
    List<String> methods = new ArrayList<>();

    if (pf.getEmailstatus() != null || pf.getArchivestatus() != null) {
        methods.add("EMAIL");
        methods.add("ARCHIVE");
    } else if (pf.getMobstatstatus() != null || pf.getArchivestatus() != null) {
        methods.add("MOBSTAT");
        methods.add("ARCHIVE");
    } else if (pf.getPrintstatus() != null || pf.getArchivestatus() != null) {
        methods.add("PRINT");
        methods.add("ARCHIVE");
    }

    int successCount = 0;
    int failedInErrorMapCount = 0;
    int failedNotInErrorMapCount = 0;

    for (String method : methods) {
        String status = getStatusByMethod(pf, method);
        if ("SUCCESS".equalsIgnoreCase(status)) {
            successCount++;
        } else if ("FAILED".equalsIgnoreCase(status)) {
            if (errorMap.containsKey(method)) {
                failedInErrorMapCount++;
            } else {
                failedNotInErrorMapCount++;
            }
        }
    }

    if (successCount == methods.size()) {
        return "SUCCESS";
    } else if (failedInErrorMapCount > 0) {
        return "FAILED";
    } else {
        return "PARTIAL";
    }
}

private static String getStatusByMethod(ProcessedFileEntry pf, String method) {
    switch (method.toUpperCase()) {
        case "EMAIL": return pf.getEmailstatus();
        case "ARCHIVE": return pf.getArchivestatus();
        case "MOBSTAT": return pf.getMobstatstatus();
        case "PRINT": return pf.getPrintstatus();
        default: return null;
    }
}
