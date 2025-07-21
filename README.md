private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
    Map<String, Map<String, String>> errorMap = new HashMap<>();

    try {
        Optional<Path> errorReportPath = findFilePath(msg, "ErrorReport.csv");
        if (errorReportPath.isEmpty()) {
            logger.info("⚠️ No ErrorReport.csv found for batch ID {}", msg.getBatchId());
            return errorMap;
        }

        Path path = errorReportPath.get();
        logger.info("📄 Parsing ErrorReport from: {}", path.toString());

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");

                if (parts.length >= 5) {
                    // Format: acc|accNum|some_field|method|status
                    String acc = parts[0].trim();
                    String method = parts[3].trim().toUpperCase();
                    String status = parts[4].trim();
                    errorMap.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                } else if (parts.length == 4) {
                    // Format: acc|accNum|method|status
                    String acc = parts[0].trim();
                    String method = parts[2].trim().toUpperCase();
                    String status = parts[3].trim();
                    errorMap.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                } else {
                    logger.warn("❗ Skipping malformed ErrorReport line: {}", line);
                }
            }
        }

        logger.info("✅ Parsed ErrorReport.csv: {} accounts with errors", errorMap.size());
        errorMap.forEach((acc, methodMap) ->
            logger.debug("➡️ Account: {}, ErrorMethods: {}", acc, methodMap)
        );

    } catch (IOException e) {
        logger.error("❌ Failed to parse ErrorReport.csv for batch {}: {}", msg.getBatchId(), e.getMessage(), e);
    }

    return errorMap;
}

===========

private static ProcessedFileEntry buildProcessedFileEntry(
    String customerId,
    String accountNumber,
    List<SummaryProcessedFile> files,
    Map<String, String> errorMap
) {
    ProcessedFileEntry entry = new ProcessedFileEntry();
    entry.setCustomerId(customerId);
    entry.setAccountNumber(accountNumber);

    Map<String, SummaryProcessedFile> typeMap = files.stream()
        .collect(Collectors.toMap(SummaryProcessedFile::getOutputType, f -> f, (f1, f2) -> f1));

    // EMAIL
    SummaryProcessedFile email = typeMap.get("EMAIL");
    if (email != null && isValidUrl(email.getBlobUrl())) {
        entry.setPdfEmailFileUrl(email.getBlobUrl());
        entry.setPdfEmailFileUrlStatus("SUCCESS");
    } else {
        entry.setPdfEmailFileUrl(null);
        entry.setPdfEmailFileUrlStatus("NOT_FOUND");
    }

    // ARCHIVE
    SummaryProcessedFile archive = typeMap.get("ARCHIVE");
    if (archive != null && isValidUrl(archive.getBlobUrl())) {
        entry.setPdfArchiveFileUrl(archive.getBlobUrl());
        entry.setPdfArchiveFileUrlStatus("SUCCESS");
    } else {
        entry.setPdfArchiveFileUrl(null);
        entry.setPdfArchiveFileUrlStatus("NOT_FOUND");

        // Special check: if EMAIL is missing but ARCHIVE found and errorMap has message
        if (email == null || !isValidUrl(email.getBlobUrl())) {
            String key = customerId + "::" + accountNumber + "::ARCHIVE";
            if (errorMap.containsKey(key)) {
                entry.setPdfArchiveFileUrlStatus("FAILED");
            }
        }
    }

    // MOBSTAT
    SummaryProcessedFile mobstat = typeMap.get("MOBSTAT");
    if (mobstat != null && isValidUrl(mobstat.getBlobUrl())) {
        entry.setPdfMobstatFileUrl(mobstat.getBlobUrl());
        entry.setPdfMobstatFileUrlStatus("SUCCESS");
    } else {
        entry.setPdfMobstatFileUrl(null);
        entry.setPdfMobstatFileUrlStatus("NOT_FOUND");

        if (mobstat == null || !isValidUrl(mobstat.getBlobUrl())) {
            String key = customerId + "::" + accountNumber + "::MOBSTAT";
            if (errorMap.containsKey(key)) {
                entry.setPdfMobstatFileUrlStatus("FAILED");
            }
        }
    }

    // PRINT
    SummaryProcessedFile print = typeMap.get("PRINT");
    if (print != null && isValidUrl(print.getBlobUrl())) {
        entry.setPrintFileUrl(print.getBlobUrl());
        entry.setPrintFileUrlStatus("SUCCESS");
    } else {
        entry.setPrintFileUrl(null);
        entry.setPrintFileUrlStatus("NOT_FOUND");

        if (print == null || !isValidUrl(print.getBlobUrl())) {
            String key = customerId + "::" + accountNumber + "::PRINT";
            if (errorMap.containsKey(key)) {
                entry.setPrintFileUrlStatus("FAILED");
            }
        }
    }

    // Final Overall Status
    Set<String> statuses = new HashSet<>(Arrays.asList(
        entry.getPdfEmailFileUrlStatus(),
        entry.getPdfArchiveFileUrlStatus(),
        entry.getPdfMobstatFileUrlStatus(),
        entry.getPrintFileUrlStatus()
    ));

    if (statuses.size() == 1 && statuses.contains("SUCCESS")) {
        entry.setOverAllStatusCode("SUCCESS");
    } else if (statuses.stream().allMatch(s -> s.equals("NOT_FOUND") || s.equals("SUCCESS"))) {
        entry.setOverAllStatusCode("PARTIAL");
    } else {
        entry.setOverAllStatusCode("FAILED");
    }

    return entry;
}
=========
private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
        Map<String, Map<String, String>> map = new HashMap<>();
        Path errorPath = Paths.get(mountPath, "output", msg.getSourceSystem(), msg.getJobName(), "ErrorReport.csv");

        if (!Files.exists(errorPath)) return map;

        try (BufferedReader reader = Files.newBufferedReader(errorPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 5) {
                    String acc = parts[0].trim();
                    String method = parts[3].trim().toUpperCase();
                    String status = parts[4].trim();
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                } else if (parts.length >= 3) {
                    String acc = parts[0].trim();
                    String method = parts[2].trim().toUpperCase();
                    String status = parts.length > 3 ? parts[3].trim() : "Failed";
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                }
            }
        } catch (Exception e) {
            logger.warn("⚠️ Error reading ErrorReport.csv", e);
        }
        return map;
    }

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

                // Record method-level status only if at least one file is present
                if (deliveryStatus != null || archiveStatus != null) {
                    if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                        statuses.add("SUCCESS");
                    } else if ("FAILED".equalsIgnoreCase(deliveryStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
                        statuses.add("FAILED");
                    } else {
                        statuses.add("PARTIAL");
                    }
                }
            }

            // ✅ Skip if no file for this customer is SUCCESS
            if (!hasSuccess) continue;

            // ✅ Determine overall status
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

