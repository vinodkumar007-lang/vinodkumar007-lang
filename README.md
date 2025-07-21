private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    Map<String, ProcessedFileEntry> entryMap = new LinkedHashMap<>();

    Map<String, List<SummaryProcessedFile>> groupedFiles = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber(), LinkedHashMap::new, Collectors.toList()));

    for (Map.Entry<String, List<SummaryProcessedFile>> group : groupedFiles.entrySet()) {
        String key = group.getKey();
        List<SummaryProcessedFile> files = group.getValue();
        String[] parts = key.split("::", 2);

        String customerId = parts[0];
        String accountNumber = parts[1];

        for (String method : List.of("email", "mobstat", "print")) {
            SummaryProcessedFile deliveryFile = null;
            SummaryProcessedFile archiveFile = null;

            for (SummaryProcessedFile file : files) {
                String outputMethod = file.getOutputMethod();
                if (outputMethod == null) continue;

                if (outputMethod.equalsIgnoreCase(method)) {
                    deliveryFile = file;
                } else if (outputMethod.equalsIgnoreCase("archive")
                        && method.equalsIgnoreCase(file.getLinkedDeliveryType())) {
                    archiveFile = file;
                }
            }

            if (deliveryFile != null || archiveFile != null) {
                ProcessedFileEntry entry = new ProcessedFileEntry();
                entry.setCustomerId(customerId);
                entry.setAccountNumber(accountNumber);

                // delivery part
                if (deliveryFile != null) {
                    String status = deliveryFile.getStatus();
                    String blobURL = deliveryFile.getBlobURL();

                    switch (method) {
                        case "email" -> {
                            entry.setPdfEmailFileUrl(blobURL);
                            entry.setPdfEmailFileUrlStatus(status);
                        }
                        case "mobstat" -> {
                            entry.setPdfMobstatFileUrl(blobURL);
                            entry.setPdfMobstatFileUrlStatus(status);
                        }
                        case "print" -> {
                            entry.setPrintFileUrl(blobURL);
                            entry.setPrintFileUrlStatus(status);
                        }
                    }
                    if ("FAILED".equalsIgnoreCase(status)) {
                        entry.setReason(deliveryFile.getStatusDescription());
                    }
                }

                // archive part
                if (archiveFile != null) {
                    String archStatus = archiveFile.getStatus();
                    String archUrl = archiveFile.getBlobURL();
                    entry.setPdfArchiveFileUrl(archUrl);
                    entry.setPdfArchiveFileUrlStatus(archStatus);

                    if ("FAILED".equalsIgnoreCase(archStatus)) {
                        entry.setReason(archiveFile.getStatusDescription());
                    }
                }

                // ✅ Compute overallStatus
                boolean deliverySuccess = deliveryFile != null && "SUCCESS".equalsIgnoreCase(deliveryFile.getStatus());
                boolean archiveSuccess = archiveFile != null && "SUCCESS".equalsIgnoreCase(archiveFile.getStatus());

                String overall;
                if (deliverySuccess && archiveSuccess) {
                    overall = "SUCCESS";
                } else if (!deliverySuccess && !archiveSuccess) {
                    overall = "FAILURE";
                } else {
                    overall = "PARTIAL";
                }

                entry.setOverAllStatusCode(overall);
                entryMap.put(customerId + "::" + accountNumber + "::" + method, entry);
            }
        }
    }

    return new ArrayList<>(entryMap.values());
}

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

    Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();
    List<SummaryProcessedFile> resultList = new ArrayList<>();

    for (SummaryProcessedFile base : customerList) {
        String customerId = base.getCustomerId();
        String accountNumber = base.getAccountNumber();

        for (String folder : folders) {
            String outputMethod = folderToOutputMethod.get(folder);
            SummaryProcessedFile spf = new SummaryProcessedFile();
            BeanUtils.copyProperties(base, spf);
            spf.setOutputMethod(outputMethod);

            String key = customerId + "::" + accountNumber + "::" + outputMethod;
            String blobUrl = getBlobUrlForCustomerAccount(folder, jobDir, customerId, accountNumber);
            spf.setBlobUrl(blobUrl);

            if (blobUrl != null) {
                spf.setStatus("SUCCESS");
            } else {
                spf.setStatus("FAILED");
            }

            if (outputMethod.equals("ARCHIVE")) {
                archiveMap.put(customerId + "::" + accountNumber, spf);
                continue; // don't add ARCHIVE directly yet
            }

            // Pair this EMAIL/MOBSTAT/PRINT with its ARCHIVE (if exists)
            SummaryProcessedFile archiveSpf = archiveMap.get(customerId + "::" + accountNumber);

            SummaryProcessedFile combined = new SummaryProcessedFile();
            combined.setCustomerId(customerId);
            combined.setAccountNumber(accountNumber);

            combined.setOutputMethod(outputMethod);
            combined.setBlobUrl(spf.getBlobUrl());
            combined.setStatus(spf.getStatus());

            combined.setArchiveOutputMethod("ARCHIVE");
            if (archiveSpf != null) {
                combined.setArchiveBlobUrl(archiveSpf.getBlobUrl());
                combined.setArchiveStatus(archiveSpf.getStatus());
            } else {
                combined.setArchiveBlobUrl(null);
                combined.setArchiveStatus("FAILED");
            }

            // Set overallStatus
            if ("SUCCESS".equals(spf.getStatus()) && "SUCCESS".equals(combined.getArchiveStatus())) {
                combined.setOverallStatus("SUCCESS");
            } else if ("FAILED".equals(spf.getStatus()) && "FAILED".equals(combined.getArchiveStatus())) {
                combined.setOverallStatus("FAILED");
            } else {
                combined.setOverallStatus("PARTIAL");
            }

            resultList.add(combined);
        }
    }

    return resultList;
}
