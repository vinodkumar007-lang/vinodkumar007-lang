 private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();
        List<String> deliveryFolders = List.of("email", "mobstat", "print");
        Map<String, String> folderToOutputMethod = Map.of(
                "email", "EMAIL",
                "mobstat", "MOBSTAT",
                "print", "PRINT"
        );

        Path archivePath = jobDir.resolve("archive");

        for (SummaryProcessedFile customer : customerList) {
            String account = customer.getAccountNumber();
            String customerId = customer.getCustomerId();

            // Step 1: Archive (always try to add)
            String archiveBlobUrl = null;
            if (Files.exists(archivePath)) {
                Optional<Path> archiveFile = Files.list(archivePath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (archiveFile.isPresent()) {
                    archiveBlobUrl = blobStorageService.uploadFileByMessage(
                            archiveFile.get().toFile(), "archive", msg);
                }
            }

            // Step 2: Delivery types - email, mobstat, print
            for (String folder : deliveryFolders) {
                String outputMethod = folderToOutputMethod.get(folder);
                Path methodPath = jobDir.resolve(folder);

                String blobUrl = "";
                String deliveryStatus = "SUCCESS"; // default to SUCCESS unless errorMap says FAILED

                if (Files.exists(methodPath)) {
                    Optional<Path> match = Files.list(methodPath)
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (match.isPresent()) {
                        blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                    } else {
                        // check errorMap
                        Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());
                        String errorStatus = customerErrors.getOrDefault(outputMethod, null);
                        if ("FAILED".equalsIgnoreCase(errorStatus)) {
                            deliveryStatus = "FAILED";
                        }
                    }
                } else {
                    // folder itself missing
                    Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());
                    String errorStatus = customerErrors.getOrDefault(outputMethod, null);
                    if ("FAILED".equalsIgnoreCase(errorStatus)) {
                        deliveryStatus = "FAILED";
                    }
                }

                // Finalize overallStatus
                String overallStatus;
                if ("FAILED".equals(deliveryStatus)) {
                    overallStatus = "FAILED";
                } else if (archiveBlobUrl != null && "SUCCESS".equals(deliveryStatus)) {
                    overallStatus = "SUCCESS";
                } else {
                    overallStatus = "PARTIAL";
                }

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputType(outputMethod);
                entry.setBlobURL(blobUrl);
                entry.setStatus(deliveryStatus);
                entry.setArchiveOutputType("ARCHIVE");
                entry.setArchiveBlobUrl(archiveBlobUrl);
                entry.setArchiveStatus(archiveBlobUrl != null ? "SUCCESS" : "NOT-FOUND");
                entry.setOverallStatus(overallStatus);

                finalList.add(entry);
            }
        }

        return finalList;
    }
