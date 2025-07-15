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
                    String method = parts[2].trim().toUpperCase();
                    String status = parts[4].trim();
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                }
            }
        } catch (Exception e) {
            logger.warn("⚠️ Error reading ErrorReport", e);
        }
        return map;
    }

    private List<SummaryProcessedFile> buildDetailedProcessedFiles(Path jobDir, Map<String, SummaryProcessedFile> customerMap, Map<String, Map<String, String>> errorMap, KafkaMessage msg) throws IOException {
        List<SummaryProcessedFile> result = new ArrayList<>();
        Set<String> folders = Set.of("email", "archive", "html", "mobstat", "txt");

        for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
            String account = entry.getKey();
            SummaryProcessedFile spf = entry.getValue();

            for (String folder : folders) {
                Path folderPath = jobDir.resolve(folder);
                if (Files.exists(folderPath)) {
                    Optional<Path> fileOpt = Files.list(folderPath)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (fileOpt.isPresent()) {
                        Path file = fileOpt.get();
                        String blobUrl = blobStorageService.uploadFile(file.toFile(), msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName());
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
                    } else {
                        Map<String, String> err = errorMap.get(account);
                        if (err != null) {
                            String outputMethod = folder.toUpperCase();
                            String status = err.getOrDefault(outputMethod, "");
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

            boolean hasFailure = Stream.of(
                    spf.getPdfEmailStatus(), spf.getPdfArchiveStatus(), spf.getHtmlEmailStatus(),
                    spf.getPdfMobstatStatus(), spf.getTxtEmailStatus()
            ).anyMatch(s -> "NOT-OK".equalsIgnoreCase(s));

            boolean allNull = Stream.of(
                    spf.getPdfEmailStatus(), spf.getPdfArchiveStatus(), spf.getHtmlEmailStatus(),
                    spf.getPdfMobstatStatus(), spf.getTxtEmailStatus()
            ).allMatch(Objects::isNull);

            if (allNull) {
                spf.setStatusCode("FAILURE");
                spf.setStatusDescription("No files processed");
            } else if (hasFailure) {
                spf.setStatusCode("PARTIAL");
                spf.setStatusDescription("Some files missing");
            } else {
                spf.setStatusCode("OK");
                spf.setStatusDescription("Success");
            }

            result.add(spf);
        }

        return result;
    }

    private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
        List<PrintFile> printFiles = new ArrayList<>();
        Path printDir = jobDir.resolve("print");
        if (!Files.exists(printDir)) return printFiles;
        try (Stream<Path> stream = Files.list(printDir)) {
            stream.filter(Files::isRegularFile).forEach(f -> {
                try {
                    String blob = blobStorageService.uploadFile(f.toFile(), msg.getSourceSystem() + "/print/" + f.getFileName());
                    printFiles.add(new PrintFile(blob));
                } catch (Exception e) {
                    logger.warn("⚠️ Print upload failed", e);
                }
            });
        } catch (IOException ignored) {}
        return printFiles;
    }
