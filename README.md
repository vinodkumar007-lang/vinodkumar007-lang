private List<SummaryProcessedFile> buildDetailedProcessedFiles(Path jobDir,
                                                               Map<String, SummaryProcessedFile> customerMap,
                                                               Map<String, Map<String, String>> errorMap,
                                                               KafkaMessage msg) throws IOException {
    List<SummaryProcessedFile> result = new ArrayList<>();
    Set<String> folders = Set.of("email", "archive", "html", "mobstat", "txt");

    for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
        String account = entry.getKey();
        SummaryProcessedFile spf = entry.getValue();
        boolean hasAnyFile = false;

        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);
            if (Files.exists(folderPath)) {
                Optional<Path> fileOpt = Files.list(folderPath)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (fileOpt.isPresent()) {
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
                        case "html" -> {
                            spf.setHtmlEmailFileUrl(decoded);
                            spf.setHtmlEmailStatus("OK");
                        }
                        case "mobstat" -> {
                            spf.setPdfMobstatFileUrl(decoded);
                            spf.setPdfMobstatStatus("OK");
                        }
                        case "txt" -> {
                            spf.setTxtEmailFileUrl(decoded);
                            spf.setTxtEmailStatus("OK");
                        }
                    }
                    hasAnyFile = true;
                } else {
                    // File not found in folder, check error report
                    Map<String, String> errorEntry = errorMap.get(account);
                    if (errorEntry != null) {
                        String status = errorEntry.getOrDefault(folder.toUpperCase(), "");
                        if (status.equalsIgnoreCase("Failed")) {
                            switch (folder) {
                                case "email" -> spf.setPdfEmailStatus("Failed");
                                case "archive" -> spf.setPdfArchiveStatus("Failed");
                                case "html" -> spf.setHtmlEmailStatus("Failed");
                                case "mobstat" -> spf.setPdfMobstatStatus("Failed");
                                case "txt" -> spf.setTxtEmailStatus("Failed");
                            }
                        }
                    }
                }
            }
        }

        // Determine final status for the customer
        List<String> statuses = List.of(
                spf.getPdfEmailStatus(), spf.getPdfArchiveStatus(), spf.getHtmlEmailStatus(),
                spf.getPdfMobstatStatus(), spf.getTxtEmailStatus()
        );

        boolean allNull = statuses.stream().allMatch(Objects::isNull);
        boolean anyFailed = statuses.stream().anyMatch("Failed"::equalsIgnoreCase);
        boolean allOk = statuses.stream().filter(Objects::nonNull).allMatch("OK"::equalsIgnoreCase);

        if (allNull) {
            spf.setStatusCode("FAILURE");
            spf.setStatusDescription("No files processed");
        } else if (anyFailed) {
            spf.setStatusCode("PARTIAL");
            spf.setStatusDescription("Some files missing");
        } else if (allOk) {
            spf.setStatusCode("OK");
            spf.setStatusDescription("Success");
        } else {
            spf.setStatusCode("PARTIAL");
            spf.setStatusDescription("Some files missing");
        }

        result.add(spf);
    }

    return result;
}
