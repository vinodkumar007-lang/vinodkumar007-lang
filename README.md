private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    // Filter valid entries
    List<SummaryProcessedFile> valid = processedList.stream()
        .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null && f.getOutputMethod() != null)
        .toList();

    // Separate archive entries
    Map<String, SummaryProcessedFile> archiveMap = valid.stream()
        .filter(f -> "ARCHIVE".equalsIgnoreCase(f.getOutputMethod()) && f.getLinkedDeliveryType() != null)
        .collect(Collectors.toMap(
            f -> f.getCustomerId() + "::" + f.getAccountNumber() + "::" + f.getLinkedDeliveryType().toUpperCase(),
            f -> f,
            (a, b) -> a
        ));

    // Process EMAIL, MOBSTAT, PRINT
    for (SummaryProcessedFile file : valid) {
        String type = file.getOutputMethod().toUpperCase();
        if (!Set.of("EMAIL", "MOBSTAT", "PRINT").contains(type)) continue;

        String key = file.getCustomerId() + "::" + file.getAccountNumber() + "::" + type;
        SummaryProcessedFile archive = archiveMap.get(key);

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());
        entry.setOverAllStatusCode("FAILED"); // default

        String deliveryStatus = file.getStatus();
        String archiveStatus = archive != null ? archive.getStatus() : null;
        String deliveryUrl = file.getBlobURL();
        String archiveUrl = archive != null ? archive.getBlobURL() : null;

        // Set fields based on delivery type
        switch (type) {
            case "EMAIL" -> {
                entry.setPdfEmailFileUrl(deliveryUrl);
                entry.setPdfEmailFileUrlStatus(deliveryStatus);
            }
            case "MOBSTAT" -> {
                entry.setPdfMobstatFileUrl(deliveryUrl);
                entry.setPdfMobstatFileUrlStatus(deliveryStatus);
            }
            case "PRINT" -> {
                entry.setPrintFileUrl(deliveryUrl);
                entry.setPrintFileUrlStatus(deliveryStatus);
            }
        }

        // Archive common field
        if (archiveUrl != null) entry.setPdfArchiveFileUrl(archiveUrl);
        if (archiveStatus != null) entry.setPdfArchiveFileUrlStatus(archiveStatus);

        // Failure reasons
        if ("FAILED".equalsIgnoreCase(deliveryStatus)) {
            entry.setReason(file.getStatusDescription());
        } else if (archive != null && "FAILED".equalsIgnoreCase(archiveStatus)) {
            entry.setReason(archive.getStatusDescription());
        }

        // Determine overallStatus
        if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
            entry.setOverAllStatusCode("SUCCESS");
        } else if ("SUCCESS".equalsIgnoreCase(deliveryStatus) || "SUCCESS".equalsIgnoreCase(archiveStatus)) {
            entry.setOverAllStatusCode("PARTIAL");
        }

        finalList.add(entry);
    }

    return finalList;
}
