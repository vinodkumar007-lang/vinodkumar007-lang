private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles) {
    Map<String, List<SummaryProcessedFile>> grouped = processedFiles.stream()
            .collect(Collectors.groupingBy(p -> p.getCustomerId() + "|" + p.getAccountNumber()));

    List<ProcessedFileEntry> result = new ArrayList<>();

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        List<SummaryProcessedFile> groupList = entry.getValue();

        if (groupList.isEmpty()) continue;

        SummaryProcessedFile first = groupList.get(0);

        ProcessedFileEntry processedEntry = new ProcessedFileEntry();
        processedEntry.setCustomerId(first.getCustomerId());
        processedEntry.setAccountNumber(first.getAccountNumber());

        String overallStatus = "SUCCESS";

        for (SummaryProcessedFile file : groupList) {
            switch (file.getOutputType()) {
                case "EMAIL":
                    processedEntry.setPdfEmailFileUrl(file.getBlobURL());
                    processedEntry.setPdfEmailFileUrlStatus(file.getStatus());
                    break;
                case "MOBSTAT":
                    processedEntry.setPdfMobstatFileUrl(file.getBlobURL());
                    processedEntry.setPdfMobstatFileUrlStatus(file.getStatus());
                    break;
                case "PRINT":
                    processedEntry.setPrintFileUrl(file.getBlobURL());
                    processedEntry.setPrintFileUrlStatus(file.getStatus());
                    break;
            }

            // Set archive only once (same for all rows)
            if (file.getArchiveBlobUrl() != null) {
                processedEntry.setArchiveBlobUrl(file.getArchiveBlobUrl());
                processedEntry.setArchiveStatus(file.getArchiveStatus());
            }

            // Compute overall status
            if ("FAILED".equalsIgnoreCase(file.getStatus())) {
                overallStatus = "FAILED";
            } else if ("NOT-FOUND".equalsIgnoreCase(file.getStatus()) && !"FAILED".equalsIgnoreCase(overallStatus)) {
                overallStatus = "PARTIAL";
            }
        }

        processedEntry.setOverallStatus(overallStatus);
        result.add(processedEntry);
    }

    return result;
}
