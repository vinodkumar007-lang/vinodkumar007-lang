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

    ==============

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
            boolean hasAtLeastOneSuccess = false;
            Map<String, Boolean> methodAdded = new HashMap<>();

            for (String folder : folders) {
                methodAdded.put(folder, false); // to prevent duplicates
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

                    // 🔗 Set linked delivery type for archive
                    if ("archive".equals(folder)) {
                        // Try to determine if archive matches email/mobstat/print by checking jobDir subfolders
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
                    hasAtLeastOneSuccess = true;
                    methodAdded.put(folder, true);
                }
            }

            // Add failed entries for folders that exist but file is missing
            for (String folder : folders) {
                if (methodAdded.get(folder)) continue;

                Path folderPath = jobDir.resolve(folder);
                if (Files.exists(folderPath)) {
                    SummaryProcessedFile failedEntry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, failedEntry);
                    failedEntry.setOutputMethod(folderToOutputMethod.get(folder));
                    failedEntry.setStatus("FAILED");
                    failedEntry.setStatusDescription("File not found for method: " + folder);
                    failedEntry.setReason("File not found in " + folder + " folder");
                    failedEntry.setBlobURL(null);

                    // ⛔ Also set linked delivery type if archive failed
                    if ("archive".equals(folder)) {
                        for (String deliveryFolder : List.of("email", "mobstat", "print")) {
                            Path deliveryPath = jobDir.resolve(deliveryFolder);
                            if (Files.exists(deliveryPath)) {
                                boolean found = Files.list(deliveryPath)
                                        .filter(Files::isRegularFile)
                                        .anyMatch(p -> p.getFileName().toString().contains(account));
                                if (found) {
                                    failedEntry.setLinkedDeliveryType(deliveryFolder.toUpperCase());
                                    break;
                                }
                            }
                        }
                    }

                    finalList.add(failedEntry);
                }
            }

            // Error report failure if any
            if (errorMap.containsKey(account)) {
                Map<String, String> errData = errorMap.get(account);
                SummaryProcessedFile errorEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, errorEntry);
                errorEntry.setOutputMethod("ERROR_REPORT");
                errorEntry.setStatus("FAILED");
                errorEntry.setStatusDescription("Marked as failed from ErrorReport");
                errorEntry.setReason(errData.get("reason"));
                errorEntry.setBlobURL(errData.get("blobURL"));
                finalList.add(errorEntry);
            }
        }

        return finalList;
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
