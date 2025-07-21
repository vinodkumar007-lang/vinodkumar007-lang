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
