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

    Map<String, SummaryProcessedFile> groupedMap = new LinkedHashMap<>();

    for (SummaryProcessedFile spf : customerList) {
        String account = spf.getAccountNumber();
        String customer = spf.getCustomerId();
        if (account == null || account.isBlank()) continue;

        String key = account + "::" + customer;
        SummaryProcessedFile existing = groupedMap.computeIfAbsent(key, k -> {
            SummaryProcessedFile newSpf = new SummaryProcessedFile();
            BeanUtils.copyProperties(spf, newSpf);
            return newSpf;
        });

        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            try (Stream<Path> files = Files.list(folderPath)) {
                Optional<Path> fileOpt = files
                        .filter(p -> p.getFileName().toString().contains(account))
                        .filter(p -> {
                            // ❌ Exclude trigger files in mobstat folder
                            if (folder.equals("mobstat") &&
                                    p.getFileName().toString().toLowerCase().contains("trigger")) {
                                return false;
                            }
                            return true;
                        })
                        .findFirst();

                String outputMethod = folderToOutputMethod.get(folder);
                Map<String, String> errorEntry = errorMap.getOrDefault(account, Collections.emptyMap());
                String failureStatus = errorEntry.getOrDefault(outputMethod, "");

                if (fileOpt.isPresent()) {
                    Path file = fileOpt.get();
                    String blobUrl = blobStorageService.uploadFile(
                            file.toFile(),
                            msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName()
                    );
                    String decoded = decodeUrl(blobUrl);

                    switch (folder) {
                        case "email" -> {
                            existing.setPdfEmailFileUrl(decoded);
                            existing.setPdfEmailStatus("OK");
                        }
                        case "archive" -> {
                            existing.setPdfArchiveFileUrl(decoded);
                            existing.setPdfArchiveStatus("OK");
                        }
                        case "mobstat" -> {
                            existing.setPdfMobstatFileUrl(decoded);
                            existing.setPdfMobstatStatus("OK");
                        }
                        case "print" -> {
                            existing.setPrintFileUrl(decoded);
                            existing.setPrintStatus("OK");
                        }
                    }
                } else {
                    boolean isExplicitFail = "Failed".equalsIgnoreCase(failureStatus);
                    switch (folder) {
                        case "email" -> existing.setPdfEmailStatus(isExplicitFail ? "Failed" : "");
                        case "archive" -> existing.setPdfArchiveStatus(isExplicitFail ? "Failed" : "");
                        case "mobstat" -> existing.setPdfMobstatStatus(isExplicitFail ? "Failed" : "");
                        case "print" -> existing.setPrintStatus(isExplicitFail ? "Failed" : "");
                    }
                }

            } catch (IOException e) {
                logger.warn("Could not read folder '{}': {}", folderPath, e.getMessage());
            }
        }

        // 🛑 Skip customers with no delivery
        List<String> statuses = Arrays.asList(
                existing.getPdfEmailStatus(),
                existing.getPdfArchiveStatus(),
                existing.getPdfMobstatStatus(),
                existing.getPrintStatus()
        );
        boolean noDelivery = statuses.stream().allMatch(s -> s == null || s.isBlank());
        if (noDelivery) {
            groupedMap.remove(key);
            continue;
        }

        // ✅ Final status
        long failed = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
        long success = statuses.stream().filter("OK"::equalsIgnoreCase).count();

        if (failed > 0 && success == 0) {
            existing.setStatusCode("FAILED");
            existing.setStatusDescription("All methods failed");
        } else if (failed > 0) {
            existing.setStatusCode("PARTIAL");
            existing.setStatusDescription("Some methods failed");
        } else {
            existing.setStatusCode("SUCCESS");
            existing.setStatusDescription("Success");
        }
    }

    return new ArrayList<>(groupedMap.values());
}
