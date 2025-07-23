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

            // Upload archive once per customer
            String archiveBlobUrl = null;
            String archiveStatus = "NOT-FOUND";

            if (Files.exists(archivePath)) {
                Optional<Path> archiveFile = Files.list(archivePath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (archiveFile.isPresent()) {
                    archiveBlobUrl = blobStorageService.uploadFileByMessage(
                            archiveFile.get().toFile(), "archive", msg);
                    archiveStatus = "SUCCESS";

                    SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, archiveEntry);
                    archiveEntry.setOutputType("ARCHIVE");
                    archiveEntry.setBlobUrl(archiveBlobUrl);
                    archiveEntry.setStatus(archiveStatus);
                    archiveEntry.setOverallStatus(archiveStatus);
                    finalList.add(archiveEntry);
                }
            }

            // Process EMAIL, MOBSTAT, PRINT
            for (String folder : deliveryFolders) {
                String outputMethod = folderToOutputMethod.get(folder);
                Path methodPath = jobDir.resolve(folder);

                String blobUrl = "";
                String deliveryStatus;
                boolean fileFound = false;

                if (Files.exists(methodPath)) {
                    Optional<Path> match = Files.list(methodPath)
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (match.isPresent()) {
                        blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                        fileFound = true;
                    }
                }

                Map<String, String> customerErrors = errorMap.getOrDefault(account, Collections.emptyMap());

                if (fileFound) {
                    deliveryStatus = "SUCCESS";
                } else if (customerErrors.containsKey(outputMethod) &&
                        "FAILED".equalsIgnoreCase(customerErrors.get(outputMethod))) {
                    deliveryStatus = "FAILED";
                } else if (!fileFound && (!customerErrors.containsKey(outputMethod) || customerErrors.get(outputMethod).isBlank())) {
                    deliveryStatus = "NOT_FOUND";
                } else {
                    deliveryStatus = "FAILED"; // fallback safety (should rarely hit)
                }

                String overallStatus = deliveryStatus;

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputType(outputMethod);
                entry.setBlobUrl(blobUrl);
                entry.setStatus(deliveryStatus);
                entry.setArchiveOutputType("ARCHIVE");
                entry.setArchiveBlobUrl(archiveBlobUrl);
                entry.setArchiveStatus(archiveStatus);
                entry.setOverallStatus(overallStatus);

                finalList.add(entry);
            }
        }

        return finalList;
    }
