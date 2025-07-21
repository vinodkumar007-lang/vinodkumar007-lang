private List<SummaryProcessedFile> buildProcessedFileEntry(String customerId, String accountNumber, Map<String, String> statusMap, Map<String, String> urlMap) {
    List<SummaryProcessedFile> processedFiles = new ArrayList<>();

    // Define combinations
    List<String[]> combinations = List.of(
        new String[]{"EMAIL", "ARCHIVE"},
        new String[]{"MOBSTAT", "ARCHIVE"},
        new String[]{"PRINT", "ARCHIVE"}
    );

    for (String[] pair : combinations) {
        String type = pair[0];
        String archive = pair[1];

        // Generate 4 logical combinations for each (SUCCESS-SUCCESS, SUCCESS-FAILED, FAILED-SUCCESS, FAILED-FAILED)
        SummaryProcessedFile entry = new SummaryProcessedFile();
        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);
        entry.setOutputMethod(type + "_" + archive);

        // Set blob URLs and statuses for the relevant types only
        if ("EMAIL".equals(type)) {
            entry.setEmailBlobUrl(urlMap.getOrDefault("EMAIL", null));
            entry.setEmailStatus(statusMap.getOrDefault("EMAIL", "FAILED"));
        } else if ("MOBSTAT".equals(type)) {
            entry.setMobstatBlobUrl(urlMap.getOrDefault("MOBSTAT", null));
            entry.setMobstatStatus(statusMap.getOrDefault("MOBSTAT", "FAILED"));
        } else if ("PRINT".equals(type)) {
            entry.setPrintBlobUrl(urlMap.getOrDefault("PRINT", null));
            entry.setPrintStatus(statusMap.getOrDefault("PRINT", "FAILED"));
        }

        // Common ARCHIVE values
        entry.setArchiveBlobUrl(urlMap.getOrDefault("ARCHIVE", null));
        entry.setArchiveStatus(statusMap.getOrDefault("ARCHIVE", "FAILED"));

        // Set overallStatus strictly based on only two statuses
        String status1 = statusMap.getOrDefault(type, "FAILED");
        String status2 = statusMap.getOrDefault("ARCHIVE", "FAILED");

        if ("SUCCESS".equals(status1) && "SUCCESS".equals(status2)) {
            entry.setOverallStatus("SUCCESS");
        } else if ("FAILED".equals(status1) && "FAILED".equals(status2)) {
            entry.setOverallStatus("FAILED");
        } else {
            entry.setOverallStatus("PARTIAL");
        }

        processedFiles.add(entry);
    }

    return processedFiles;
}
