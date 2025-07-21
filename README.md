private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    Map<String, ProcessedFileEntry> entryMap = new LinkedHashMap<>();
    Map<String, List<SummaryProcessedFile>> groupedFiles = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber(), LinkedHashMap::new, Collectors.toList()));

    for (Map.Entry<String, List<SummaryProcessedFile>> group : groupedFiles.entrySet()) {
        String key = group.getKey();
        List<SummaryProcessedFile> files = group.getValue();

        ProcessedFileEntry entry = new ProcessedFileEntry();
        String[] parts = key.split("::", 2);
        entry.setCustomerId(parts[0]);
        entry.setAccountNumber(parts[1]);

        Map<String, SummaryProcessedFile> deliveryMap = new HashMap<>();
        Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();
        List<String> statuses = new ArrayList<>();

        for (SummaryProcessedFile file : files) {
            String method = file.getOutputMethod();
            if (method == null) continue;

            String status = file.getStatus() != null ? file.getStatus() : "UNKNOWN";
            statuses.add(status);

            String blobURL = file.getBlobURL();
            String reason = file.getStatusDescription();

            switch (method.toLowerCase()) {
                case "email" -> deliveryMap.put("email", file);
                case "mobstat" -> deliveryMap.put("mobstat", file);
                case "print" -> deliveryMap.put("print", file);
                case "archive" -> {
                    String linked = file.getLinkedDeliveryType();
                    if (linked != null) archiveMap.put(linked.toLowerCase(), file);
                }
            }
        }

        // 📦 Set fields and combine archive logic
        for (String method : List.of("email", "mobstat", "print")) {
            SummaryProcessedFile delivery = deliveryMap.get(method);
            SummaryProcessedFile archive = archiveMap.get(method);

            if (delivery != null) {
                String status = delivery.getStatus();
                String blobURL = delivery.getBlobURL();
                String reason = delivery.getStatusDescription();

                switch (method) {
                    case "email" -> {
                        entry.setPdfEmailFileUrl(blobURL);
                        entry.setPdfEmailFileUrlStatus(status);
                        if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                    }
                    case "mobstat" -> {
                        entry.setPdfMobstatFileUrl(blobURL);
                        entry.setPdfMobstatFileUrlStatus(status);
                        if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                    }
                    case "print" -> {
                        entry.setPrintFileUrl(blobURL);
                        entry.setPrintFileUrlStatus(status);
                        if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                    }
                }
            }

            if (archive != null) {
                String status = archive.getStatus();
                String blobURL = archive.getBlobURL();
                String reason = archive.getStatusDescription();

                if ("email".equals(method)) {
                    entry.setPdfArchiveFileUrl(blobURL);  // reused archive slot
                    entry.setPdfArchiveFileUrlStatus(status);
                    if ("FAILED".equalsIgnoreCase(status)) entry.setReason(reason);
                }
                // Optional: extend logic for separate archive types if needed
            }
        }

        // ✅ Determine overallStatusCode
        boolean allSuccess = statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s));
        boolean noneSuccess = statuses.stream().noneMatch(s -> "SUCCESS".equalsIgnoreCase(s));
        String overallStatus = allSuccess ? "SUCCESS" : noneSuccess ? "FAILURE" : "PARTIAL";
        entry.setOverAllStatusCode(overallStatus);

        entryMap.put(key, entry);
    }

    return new ArrayList<>(entryMap.values());
}
