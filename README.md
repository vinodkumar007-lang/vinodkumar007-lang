private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
        List<PrintFile> printFiles = new ArrayList<>();
        Path printFolder = jobDir.resolve("print");
        if (!Files.exists(printFolder)) return printFiles;

        try (Stream<Path> paths = Files.list(printFolder)) {
            paths.filter(Files::isRegularFile).forEach(file -> {
                try {
                    String fileName = file.getFileName().toString();
                    String remotePath = String.format("%s/%s/%s/print/%s",
                            msg.getSourceSystem(), msg.getBatchId(), msg.getUniqueConsumerRef(), fileName);

                    String blobUrl = blobStorageService.uploadFile(file.toFile(), remotePath);
                    printFiles.add(new PrintFile(blobUrl));
                } catch (Exception e) {
                    logger.error("Failed to upload print file: {}", file, e);
                }
            });
        } catch (IOException e) {
            logger.error("Error accessing print folder", e);
        }
        return printFiles;
    }
