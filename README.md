private List<SummaryProcessedFile> buildAndUploadProcessedFiles(Path jobDir, Map<String, String> accountCustomerMap, KafkaMessage msg) throws IOException {
    List<SummaryProcessedFile> list = new ArrayList<>();
    List<String> folders = List.of("archive", "email", "html", "mobstat", "txt");

    for (String folder : folders) {
        Path subDir = jobDir.resolve(folder);
        if (!Files.exists(subDir)) continue;

        Files.list(subDir).filter(Files::isRegularFile).forEach(file -> {
            try {
                String fileName = file.getFileName().toString();
                String account = extractAccountFromFileName(fileName);
                if (account == null) return;

                String customer = accountCustomerMap.get(account);

                SummaryProcessedFile entry = list.stream()
                        .filter(e -> account.equals(e.getAccountNumber()))
                        .findFirst()
                        .orElseGet(() -> {
                            SummaryProcessedFile newEntry = new SummaryProcessedFile();
                            newEntry.setAccountNumber(account);
                            newEntry.setCustomerId(customer);
                            newEntry.setStatusCode("OK");
                            newEntry.setStatusDescription("Success");
                            list.add(newEntry);
                            return newEntry;
                        });

                String blobUrl = blobStorageService.uploadFile(
                        file, // use Path for binary-safe upload
                        String.format("%s/%s/%s/%s/%s",
                                msg.getSourceSystem(),
                                msg.getBatchId(),
                                msg.getUniqueConsumerRef(),
                                folder,
                                fileName)
                );

                // Decode URL before storing
                String decodedUrl = decodeUrl(blobUrl);

                switch (folder) {
                    case "archive" -> entry.setPdfArchiveFileUrl(decodedUrl);
                    case "email" -> entry.setPdfEmailFileUrl(decodedUrl);
                    case "html" -> entry.setHtmlEmailFileUrl(decodedUrl);
                    case "txt" -> entry.setTxtEmailFileUrl(decodedUrl);
                    case "mobstat" -> entry.setPdfMobstatFileUrl(decodedUrl);
                }

            } catch (Exception e) {
                logger.error("Error uploading file: {}", file, e);
            }
        });
    }
    return list;
}
