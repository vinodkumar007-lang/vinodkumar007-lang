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

        boolean hasFailed = false;
        boolean hasPartial = false;

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

            if (file.getArchiveBlobUrl() != null) {
                processedEntry.setArchiveBlobUrl(file.getArchiveBlobUrl());
                processedEntry.setArchiveStatus(file.getArchiveStatus());
            }

            // Track statuses
            if ("FAILED".equalsIgnoreCase(file.getStatus())) {
                hasFailed = true;
            } else if ("NOT-FOUND".equalsIgnoreCase(file.getStatus())) {
                hasPartial = true;
            }
        }

        // Decide overallStatus based on severity
        if (hasFailed) {
            processedEntry.setOverallStatus("FAILED");
        } else if (hasPartial) {
            processedEntry.setOverallStatus("PARTIAL");
        } else {
            processedEntry.setOverallStatus("SUCCESS");
        }

        result.add(processedEntry);
    }

    return result;
}
