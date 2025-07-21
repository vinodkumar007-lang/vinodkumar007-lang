private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> group : grouped.entrySet()) {
        String[] parts = group.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        Map<String, SummaryProcessedFile> methodMap = new HashMap<>();
        Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();

        for (SummaryProcessedFile file : group.getValue()) {
            String method = file.getOutputMethod();
            if (method == null) continue;

            switch (method.toUpperCase()) {
                case "EMAIL", "MOBSTAT", "PRINT" -> methodMap.put(method.toUpperCase(), file);
                case "ARCHIVE" -> {
                    String linked = file.getLinkedDeliveryType();
                    if (linked != null) {
                        archiveMap.put(linked.toUpperCase(), file);
                    }
                }
            }
        }

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);

        List<String> statuses = new ArrayList<>();
        boolean hasAtLeastOneSuccess = false;

        for (String type : List.of("EMAIL", "MOBSTAT", "PRINT")) {
            SummaryProcessedFile delivery = methodMap.get(type);
            SummaryProcessedFile archive = archiveMap.get(type);

            String deliveryStatus = null, archiveStatus = null;
            String deliveryUrl = null, archiveUrl = null;
            String deliveryReason = null, archiveReason = null;

            if (delivery != null) {
                deliveryStatus = delivery.getStatus();
                deliveryUrl = delivery.getBlobURL();
                deliveryReason = delivery.getStatusDescription();
            }

            if (archive != null) {
                archiveStatus = archive.getStatus();
                archiveUrl = archive.getBlobURL();
                archiveReason = archive.getStatusDescription();
            }

            // Set file URLs and statuses
            switch (type) {
                case "EMAIL" -> {
                    entry.setPdfEmailFileUrl(deliveryUrl);
                    entry.setPdfEmailFileUrlStatus(deliveryStatus);
                    if ("FAILED".equalsIgnoreCase(deliveryStatus)) {
                        entry.setReason(deliveryReason);
                    }
                }
                case "MOBSTAT" -> {
                    entry.setPdfMobstatFileUrl(deliveryUrl);
                    entry.setPdfMobstatFileUrlStatus(deliveryStatus);
                    if ("FAILED".equalsIgnoreCase(deliveryStatus)) {
                        entry.setReason(deliveryReason);
                    }
                }
                case "PRINT" -> {
                    entry.setPrintFileUrl(deliveryUrl);
                    entry.setPrintFileUrlStatus(deliveryStatus);
                    if ("FAILED".equalsIgnoreCase(deliveryStatus)) {
                        entry.setReason(deliveryReason);
                    }
                }
            }

            // Shared archive URL + status field (overwrites last one)
            if (archiveUrl != null) {
                entry.setPdfArchiveFileUrl(archiveUrl);
            }
            if (archiveStatus != null) {
                entry.setPdfArchiveFileUrlStatus(archiveStatus);
                if ("FAILED".equalsIgnoreCase(archiveStatus) && entry.getReason() == null) {
                    entry.setReason(archiveReason);
                }
            }

            // Determine per-type outcome
            if (deliveryStatus == null && archiveStatus == null) {
                continue; // No file at all, skip
            }

            if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                statuses.add("SUCCESS");
                hasAtLeastOneSuccess = true;
            } else if ("SUCCESS".equalsIgnoreCase(deliveryStatus) || "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                statuses.add("PARTIAL");
                hasAtLeastOneSuccess = true;
            } else {
                statuses.add("FAILED");
            }
        }

        if (!hasAtLeastOneSuccess) continue; // Don't add if nothing was successful

        // Set overall status
        if (statuses.stream().allMatch(s -> "SUCCESS".equals(s))) {
            entry.setOverAllStatusCode("SUCCESS");
        } else if (statuses.stream().allMatch(s -> "FAILED".equals(s))) {
            entry.setOverAllStatusCode("FAILED");
        } else {
            entry.setOverAllStatusCode("PARTIAL");
        }

        finalList.add(entry);
    }

    return finalList;
}
