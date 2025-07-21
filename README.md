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
