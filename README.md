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

        // Create one combined entry per customer-account
        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);

        List<String> statuses = new ArrayList<>();

        for (String type : List.of("EMAIL", "MOBSTAT", "PRINT")) {
            SummaryProcessedFile delivery = methodMap.get(type);
            SummaryProcessedFile archive = archiveMap.get(type);

            String deliveryStatus = null, archiveStatus = null;

            if (delivery != null) {
                String url = delivery.getBlobURL();
                deliveryStatus = delivery.getStatus();
                String reason = delivery.getStatusDescription();

                switch (type) {
                    case "EMAIL" -> {
                        entry.setPdfEmailFileUrl(url);
                        entry.setPdfEmailFileUrlStatus(deliveryStatus);
                        if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                    }
                    case "MOBSTAT" -> {
                        entry.setPdfMobstatFileUrl(url);
                        entry.setPdfMobstatFileUrlStatus(deliveryStatus);
                        if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                    }
                    case "PRINT" -> {
                        entry.setPrintFileUrl(url);
                        entry.setPrintFileUrlStatus(deliveryStatus);
                        if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                    }
                }
            }

            if (archive != null) {
                String aStatus = archive.getStatus();
                String aUrl = archive.getBlobURL();

                switch (type) {
                    case "EMAIL" -> {
                        entry.setPdfArchiveFileUrl(aUrl); // For EMAIL
                        entry.setPdfArchiveFileUrlStatus(aStatus);
                    }
                    case "MOBSTAT" -> {
                        if (entry.getPdfArchiveFileUrl() == null) entry.setPdfArchiveFileUrl(aUrl);
                        entry.setPdfArchiveFileUrlStatus(aStatus);
                    }
                    case "PRINT" -> {
                        if (entry.getPdfArchiveFileUrl() == null) entry.setPdfArchiveFileUrl(aUrl);
                        entry.setPdfArchiveFileUrlStatus(aStatus);
                    }
                }

                archiveStatus = aStatus;
                if ("FAILED".equalsIgnoreCase(archiveStatus) && entry.getReason() == null) {
                    entry.setReason(archive.getStatusDescription());
                }
            }

            statuses.add(
                "FAILED".equalsIgnoreCase(deliveryStatus) || "FAILED".equalsIgnoreCase(archiveStatus) ? "FAILED" :
                "SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus) ? "SUCCESS" :
                "PARTIAL"
            );
        }

        // Determine overall status
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
