private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "archive", "ARCHIVE",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    List<SummaryProcessedFile> processedList = new ArrayList<>();
    Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();

    for (SummaryProcessedFile base : customerList) {
        String customerId = base.getCustomerId();
        String accountNumber = base.getAccountNumber();

        for (String folder : folders) {
            String outputMethod = folderToOutputMethod.get(folder);
            SummaryProcessedFile spf = new SummaryProcessedFile();
            BeanUtils.copyProperties(base, spf);
            spf.setOutputMethod(outputMethod);

            String blobUrl = getBlobUrlForCustomerAccount(folder, jobDir, customerId, accountNumber);
            spf.setBlobUrl(blobUrl);

            if (blobUrl != null) {
                spf.setStatus("SUCCESS");
            } else {
                spf.setStatus("FAILED");
            }

            if ("ARCHIVE".equals(outputMethod)) {
                String archiveKey = customerId + "::" + accountNumber;
                archiveMap.put(archiveKey, spf);
                continue; // skip adding ARCHIVE for now
            }

            // Attach archive info if available
            String matchKey = customerId + "::" + accountNumber;
            SummaryProcessedFile archiveSpf = archiveMap.get(matchKey);
            if (archiveSpf != null) {
                spf.setArchiveOutputMethod("ARCHIVE");
                spf.setArchiveBlobUrl(archiveSpf.getBlobUrl());
                spf.setArchiveStatus(archiveSpf.getStatus());
            } else {
                spf.setArchiveOutputMethod("ARCHIVE");
                spf.setArchiveBlobUrl(null);
                spf.setArchiveStatus("FAILED");
            }

            // Set overallStatus
            if ("SUCCESS".equals(spf.getStatus()) && "SUCCESS".equals(spf.getArchiveStatus())) {
                spf.setOverallStatus("SUCCESS");
            } else if ("FAILED".equals(spf.getStatus()) && "FAILED".equals(spf.getArchiveStatus())) {
                spf.setOverallStatus("FAILED");
            } else {
                spf.setOverallStatus("PARTIAL");
            }

            processedList.add(spf);
        }
    }

    return processedList;
}
