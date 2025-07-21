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
        boolean hasSuccess = false;

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

                if ("SUCCESS".equalsIgnoreCase(deliveryStatus)) hasSuccess = true;
            }

            if (archive != null) {
                archiveStatus = archive.getStatus();
                String aUrl = archive.getBlobURL();

                entry.setPdfArchiveFileUrl(aUrl); // shared field
                entry.setPdfArchiveFileUrlStatus(archiveStatus);

                if ("FAILED".equalsIgnoreCase(archiveStatus) && entry.getReason() == null) {
                    entry.setReason(archive.getStatusDescription());
                }

                if ("SUCCESS".equalsIgnoreCase(archiveStatus)) hasSuccess = true;
            }

            // Record method-level status for overall calculation
            if ("FAILED".equalsIgnoreCase(deliveryStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
                statuses.add("FAILED");
            } else if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                statuses.add("SUCCESS");
            } else {
                statuses.add("PARTIAL");
            }
        }

        // ✅ Filter: only include if at least one SUCCESS
        if (!hasSuccess) continue;

        // Determine overall status for entry
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
