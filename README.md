   private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            Map<String, SummaryProcessedFile> customerMap,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> result = new ArrayList<>();
        List<String> folders = List.of("email", "archive", "mobstat", "print");
        Map<String, String> folderToOutputMethod = Map.of(
                "email", "EMAIL",
                "archive", "ARCHIVE",
                "mobstat", "MOBSTAT",
                "print", "PRINT"
        );

        for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
            String account = entry.getKey();
            SummaryProcessedFile spf = entry.getValue();
            Map<String, Boolean> methodFound = new HashMap<>();

            for (String folder : folders) {
                Path folderPath = jobDir.resolve(folder);
                Optional<Path> fileOpt = Files.exists(folderPath)
                        ? Files.list(folderPath)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst()
                        : Optional.empty();

                String outputMethod = folderToOutputMethod.get(folder);
                Map<String, String> errorEntry = errorMap.getOrDefault(account, Collections.emptyMap());
                String failureStatus = errorEntry.getOrDefault(outputMethod, "");

                boolean fileFound = fileOpt.isPresent();
                methodFound.put(outputMethod, fileFound);

                if (fileFound) {
                    Path file = fileOpt.get();
                    String blobUrl = blobStorageService.uploadFile(
                            file.toFile(),
                            msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName()
                    );
                    String decoded = decodeUrl(blobUrl);

                    switch (folder) {
                        case "email" -> {
                            spf.setPdfEmailFileUrl(decoded);
                            spf.setPdfEmailStatus("OK");
                        }
                        case "archive" -> {
                            spf.setPdfArchiveFileUrl(decoded);
                            spf.setPdfArchiveStatus("OK");
                        }
                        case "mobstat" -> {
                            spf.setPdfMobstatFileUrl(decoded);
                            spf.setPdfMobstatStatus("OK");
                        }
                        case "print" -> spf.setPrintFileUrl(decoded);
                    }
                } else {
                    // file not found, check error report
                    boolean isExplicitlyFailed = "Failed".equalsIgnoreCase(failureStatus);
                    if (isExplicitlyFailed) {
                        switch (folder) {
                            case "email" -> spf.setPdfEmailStatus("Failed");
                            case "archive" -> spf.setPdfArchiveStatus("Failed");
                            case "mobstat" -> spf.setPdfMobstatStatus("Failed");
                        }
                    } else {
                        // if not in error report, leave status as empty
                        switch (folder) {
                            case "email" -> spf.setPdfEmailStatus("");
                            case "archive" -> spf.setPdfArchiveStatus("");
                            case "mobstat" -> spf.setPdfMobstatStatus("");
                        }
                    }
                }
            }

            // Final status decision logic
            List<String> statuses = Arrays.asList(
                    spf.getPdfEmailStatus(),
                    spf.getPdfArchiveStatus(),
                    spf.getPdfMobstatStatus()
            );

            long failedCount = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
            long knownCount = statuses.stream().filter(s -> s != null && !s.isBlank()).count();

            if (failedCount == knownCount && knownCount > 0) {
                spf.setStatusCode("FAILED");
                spf.setStatusDescription("All methods failed");
            } else if (failedCount > 0) {
                spf.setStatusCode("PARTIAL");
                spf.setStatusDescription("Some methods failed");
            } else {
                spf.setStatusCode("SUCCESS");
                spf.setStatusDescription("Success");
            }

            result.add(spf);
        }

        return result;
    }
