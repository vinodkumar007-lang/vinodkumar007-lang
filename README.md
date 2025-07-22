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

            // ARCHIVE always added
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

            // Process EMAIL, MOBSTAT, PRINT individually
            for (String folder : deliveryFolders) {
                String outputMethod = folderToOutputMethod.get(folder);
                Path methodPath = jobDir.resolve(folder);

                String blobUrl = null;
                String deliveryStatus = "NOT-FOUND";
                boolean fileFound = false;

                if (Files.exists(methodPath)) {
                    Optional<Path> match = Files.list(methodPath)
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (match.isPresent()) {
                        blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                        deliveryStatus = "SUCCESS";
                        fileFound = true;
                    }
                }

                if (!fileFound) {
                    Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());
                    String errorStatus = customerErrors.getOrDefault(outputMethod, null);

                    if ("FAILED".equalsIgnoreCase(errorStatus)) {
                        deliveryStatus = "FAILED";
                    }
                }

                // Decide overallStatus
                String overallStatus;
                if ("SUCCESS".equals(deliveryStatus) && archiveBlobUrl != null) {
                    overallStatus = "SUCCESS";
                } else if ("FAILED".equals(deliveryStatus)) {
                    overallStatus = "FAILED";
                } else {
                    overallStatus = "PARTIAL";
                }

                // Build and add final entry
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputType(outputMethod);
                entry.setBlobURL(blobUrl);
                entry.setStatus(deliveryStatus);
                entry.setArchiveOutputType("ARCHIVE");
                entry.setArchiveBlobUrl(archiveBlobUrl);
                entry.setArchiveStatus("SUCCESS"); // always present if archiveBlobUrl present
                entry.setOverallStatus(overallStatus);

                finalList.add(entry);
            }
        }

        return finalList;
    }
