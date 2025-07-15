private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        Map<String, SummaryProcessedFile> customerMap,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> result = new ArrayList<>();
    List<String> folders = List.of("email", "archive", "html", "mobstat", "txt");

    for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
        String account = entry.getKey();
        SummaryProcessedFile spf = entry.getValue();
        boolean hasAnyFile = false;

        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);

            Optional<Path> fileOpt = Files.exists(folderPath)
                    ? Files.list(folderPath)
                        .filter(p -> p.getFileName().toString().startsWith(account))
                        .findFirst()
                    : Optional.empty();

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
                // File not found — check error map
                Map<String, String> errorEntry = errorMap.getOrDefault(account, Collections.emptyMap());
                String failureStatus = errorEntry.getOrDefault(folder.toUpperCase(), "");

                if ("Failed".equalsIgnoreCase(failureStatus)) {
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

        // Determine overall status
        List<String> statuses = Arrays.asList(
                spf.getPdfEmailStatus(),
                spf.getPdfArchiveStatus(),
                spf.getHtmlEmailStatus(),
                spf.getPdfMobstatStatus(),
                spf.getTxtEmailStatus()
        );

        boolean allNull = statuses.stream().allMatch(Objects::isNull);
        boolean anyFailed = statuses.stream().anyMatch(s -> "Failed".equalsIgnoreCase(s));
        boolean allOk = statuses.stream().filter(Objects::nonNull).allMatch(s -> "OK".equalsIgnoreCase(s));

        if (allNull) {
            spf.setStatusCode("FAILURE");
            spf.setStatusDescription("No files processed");
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
