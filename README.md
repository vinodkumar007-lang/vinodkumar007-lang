private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
        .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
        .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> group : grouped.entrySet()) {
        String[] parts = group.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        // Create maps by output method
        Map<String, SummaryProcessedFile> methodMap = new HashMap<>();
        Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();

        for (SummaryProcessedFile file : group.getValue()) {
            String method = file.getOutputMethod();
            if (method == null) continue;

            switch (method) {
                case "EMAIL", "MOBSTAT", "PRINT" -> methodMap.put(method, file);
                case "ARCHIVE" -> {
                    String linked = file.getLinkedDeliveryType(); // email/mobstat/print
                    if (linked != null) {
                        archiveMap.put(linked.toUpperCase(), file); // key as EMAIL, etc.
                    }
                }
            }
        }

        // Now build combinations
        for (String type : List.of("EMAIL", "MOBSTAT", "PRINT")) {
            SummaryProcessedFile delivery = methodMap.get(type);
            SummaryProcessedFile archive = archiveMap.get(type);

            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(customerId);
            entry.setAccountNumber(accountNumber);

            // Set delivery file info
            if (delivery != null) {
                String url = delivery.getBlobURL();
                String status = delivery.getStatus();
                String reason = delivery.getStatusDescription();

                switch (type) {
                    case "EMAIL" -> {
                        entry.setPdfEmailFileUrl(url);
                        entry.setPdfEmailFileUrlStatus(status);
                        if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                    }
                    case "MOBSTAT" -> {
                        entry.setPdfMobstatFileUrl(url);
                        entry.setPdfMobstatFileUrlStatus(status);
                        if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                    }
                    case "PRINT" -> {
                        entry.setPrintFileUrl(url);
                        entry.setPrintFileUrlStatus(status);
                        if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                    }
                }
            }

            // Set archive info for that type
            if (archive != null) {
                entry.setPdfArchiveFileUrl(archive.getBlobURL());
                entry.setPdfArchiveFileUrlStatus(archive.getStatus());
                if ("FAILED".equalsIgnoreCase(archive.getStatus())) {
                    entry.setReason(archive.getStatusDescription());
                }
            }

            // Compute overall status for the pair
            String deliveryStatus = delivery != null ? delivery.getStatus() : "FAILED";
            String archiveStatus = archive != null ? archive.getStatus() : "FAILED";

            if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                entry.setOverAllStatusCode("SUCCESS");
            } else if ("FAILED".equalsIgnoreCase(deliveryStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
                entry.setOverAllStatusCode("FAILED");
            } else {
                entry.setOverAllStatusCode("PARTIAL");
            }

            finalList.add(entry);
        }
    }

    return finalList;
}
