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

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    for (SummaryProcessedFile customer : customerList) {
        String account = customer.getAccountNumber();
        String customerId = customer.getCustomerId();
        Map<String, Boolean> methodAdded = new HashMap<>();

        for (String folder : folders) {
            methodAdded.put(folder, false);
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            Optional<Path> match = Files.list(folderPath)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().contains(account))
                    .findFirst();

            if (match.isPresent()) {
                Path filePath = match.get();
                File file = filePath.toFile();
                String blobUrl = blobStorageService.uploadFileByMessage(file, folder, msg);

                SummaryProcessedFile successEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, successEntry);
                successEntry.setOutputMethod(folderToOutputMethod.get(folder));
                successEntry.setStatus("SUCCESS");
                successEntry.setBlobURL(blobUrl);

                if ("archive".equals(folder)) {
                    for (String deliveryFolder : List.of("email", "mobstat", "print")) {
                        Path deliveryPath = jobDir.resolve(deliveryFolder);
                        if (Files.exists(deliveryPath)) {
                            boolean found = Files.list(deliveryPath)
                                    .filter(Files::isRegularFile)
                                    .anyMatch(p -> p.getFileName().toString().contains(account));
                            if (found) {
                                successEntry.setLinkedDeliveryType(deliveryFolder.toUpperCase());
                                break;
                            }
                        }
                    }
                }

                finalList.add(successEntry);
                methodAdded.put(folder, true);
            }
        }

        for (String folder : folders) {
            if (methodAdded.get(folder)) continue;
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            String method = folderToOutputMethod.get(folder);
            entry.setOutputMethod(method);

            if (errorMap.containsKey(account) && errorMap.get(account).containsKey(method)) {
                String errorStatus = errorMap.get(account).get(method);
                entry.setStatus("FAILED".equals(errorStatus) ? "FAILED" : "PARTIAL");
                entry.setStatusDescription("Marked from ErrorReport");
            } else {
                entry.setStatus("SUCCESS"); // Not present = no error
                entry.setStatusDescription("No error found in ErrorReport");
            }

            if ("archive".equals(folder)) {
                for (String deliveryFolder : List.of("email", "mobstat", "print")) {
                    Path deliveryPath = jobDir.resolve(deliveryFolder);
                    if (Files.exists(deliveryPath)) {
                        boolean found = Files.list(deliveryPath)
                                .filter(Files::isRegularFile)
                                .anyMatch(p -> p.getFileName().toString().contains(account));
                        if (found) {
                            entry.setLinkedDeliveryType(deliveryFolder.toUpperCase());
                            break;
                        }
                    }
                }
            }

            finalList.add(entry);
        }
    }

    return finalList;
}
===========
private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> group : grouped.entrySet()) {
        String[] parts = group.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        ProcessedFileEntry entry = new ProcessedFileEntry();
        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);

        Map<String, String> methodStatus = new HashMap<>();
        for (SummaryProcessedFile spf : group.getValue()) {
            String method = spf.getOutputMethod();
            if (method == null) continue;

            String status = spf.getStatus();
            String url = spf.getBlobURL();
            String desc = spf.getStatusDescription();

            switch (method.toUpperCase()) {
                case "EMAIL" -> {
                    entry.setPdfEmailFileUrl(url);
                    entry.setPdfEmailFileUrlStatus(status);
                    if ("FAILED".equals(status)) entry.setReason(desc);
                    methodStatus.put("EMAIL", status);
                }
                case "MOBSTAT" -> {
                    entry.setPdfMobstatFileUrl(url);
                    entry.setPdfMobstatFileUrlStatus(status);
                    if ("FAILED".equals(status)) entry.setReason(desc);
                    methodStatus.put("MOBSTAT", status);
                }
                case "PRINT" -> {
                    entry.setPrintFileUrl(url);
                    entry.setPrintFileUrlStatus(status);
                    if ("FAILED".equals(status)) entry.setReason(desc);
                    methodStatus.put("PRINT", status);
                }
                case "ARCHIVE" -> {
                    entry.setPdfArchiveFileUrl(url);
                    entry.setPdfArchiveFileUrlStatus(status);
                    if ("FAILED".equals(status) && entry.getReason() == null) entry.setReason(desc);
                    methodStatus.put("ARCHIVE", status);
                }
            }
        }

        if (methodStatus.values().stream().allMatch("SUCCESS"::equals)) {
            entry.setOverAllStatusCode("SUCCESS");
        } else if (methodStatus.values().stream().noneMatch("SUCCESS"::equals)) {
            entry.setOverAllStatusCode("FAILED");
        } else {
            entry.setOverAllStatusCode("PARTIAL");
        }

        finalList.add(entry);
    }

    return finalList;
}
