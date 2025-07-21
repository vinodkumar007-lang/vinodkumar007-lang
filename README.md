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

            String method = folderToOutputMethod.get(folder);

            if (match.isPresent()) {
                Path filePath = match.get();
                File file = filePath.toFile();
                String blobUrl = blobStorageService.uploadFileByMessage(file, folder, msg);

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputMethod(method);
                entry.setBlobURL(blobUrl);

                if (errorMap.containsKey(account) && errorMap.get(account).containsKey(method)) {
                    String status = errorMap.get(account).get(method);
                    if ("FAILED".equalsIgnoreCase(status)) {
                        entry.setStatus("FAILED");
                        entry.setStatusDescription("Marked as FAILED from ErrorReport");
                    } else {
                        entry.setStatus("PARTIAL");
                        entry.setStatusDescription("Marked as PARTIAL from ErrorReport");
                    }
                } else {
                    entry.setStatus("SUCCESS");
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
                methodAdded.put(folder, true);
            }
        }

        // Add entries for errorMap methods that were not found in folders
        if (errorMap.containsKey(account)) {
            Map<String, String> methodErrors = errorMap.get(account);

            for (String folder : folders) {
                if (methodAdded.getOrDefault(folder, false)) continue;
                Path folderPath = jobDir.resolve(folder);
                if (!Files.exists(folderPath)) continue;

                String method = folderToOutputMethod.get(folder);
                if (!methodErrors.containsKey(method)) continue;

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputMethod(method);
                entry.setBlobURL(null); // file not found

                String status = methodErrors.get(method);
                if ("FAILED".equalsIgnoreCase(status)) {
                    entry.setStatus("FAILED");
                    entry.setStatusDescription("Marked as FAILED from ErrorReport (file not found)");
                } else {
                    entry.setStatus("PARTIAL");
                    entry.setStatusDescription("Marked as PARTIAL from ErrorReport (file not found)");
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
    }

    return finalList;
}

========

private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        List<SummaryProcessedFile> list = entry.getValue();
        if (list.isEmpty()) continue;

        String[] keyParts = entry.getKey().split("::");
        String customerId = keyParts[0];
        String accountNumber = keyParts[1];

        Map<String, SummaryProcessedFile> methodMap = list.stream()
                .collect(Collectors.toMap(SummaryProcessedFile::getOutputMethod, Function.identity(), (a, b) -> a));

        // Determine primary delivery method
        String deliveryMethod = methodMap.keySet().stream()
                .filter(m -> !m.equals("ARCHIVE"))
                .findFirst()
                .orElse(null);

        SummaryProcessedFile deliveryFile = methodMap.get(deliveryMethod);
        SummaryProcessedFile archiveFile = methodMap.get("ARCHIVE");

        String deliveryStatus = deliveryFile != null ? deliveryFile.getStatus() : "NOT_FOUND";
        String archiveStatus = archiveFile != null ? archiveFile.getStatus() : "NOT_FOUND";

        String overallStatus = computeOverallStatus(deliveryStatus, archiveStatus);

        ProcessedFileEntry pf = new ProcessedFileEntry();
        pf.setCustomerId(customerId);
        pf.setAccountNumber(accountNumber);
        pf.setOutputType(deliveryMethod);
        pf.setBlobUrl(deliveryFile != null ? deliveryFile.getBlobURL() : null);
        pf.setStatus(deliveryStatus);
        pf.setArchiveStatus(archiveStatus);
        pf.setOverallStatus(overallStatus);
        pf.setStatusDescription("Evaluated from delivery and archive statuses");

        finalList.add(pf);
    }

    return finalList;
}
======
private static String computeOverallStatus(String delivery, String archive) {
    Set<String> set = new HashSet<>(Arrays.asList(
        delivery == null ? "NOT_FOUND" : delivery,
        archive == null ? "NOT_FOUND" : archive
    ));

    if (set.contains("FAILED") && set.contains("SUCCESS")) return "PARTIAL";
    if (set.contains("FAILED") && set.contains("FAILED")) return "FAILED";
    if (set.contains("NOT_FOUND")) return "PARTIAL";
    if (set.size() == 1 && set.contains("SUCCESS")) return "SUCCESS";

    return "PARTIAL";
}
