   private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
        List<PrintFile> printFiles = new ArrayList<>();

        if (jobDir == null || msg == null || msg.getSourceSystem() == null) {
            logger.error("❌ Invalid input: jobDir={}, msg={}, sourceSystem={}", jobDir, msg, msg != null ? msg.getSourceSystem() : null);
            return printFiles;
        }

        Path printDir = jobDir.resolve(PRINT_FOLDER_NAME);
        if (!Files.exists(printDir)) {
            logger.info("ℹ️ No '{}' directory found in jobDir: {}", PRINT_FOLDER_NAME, jobDir);
            return printFiles;
        }

        try (Stream<Path> stream = Files.list(printDir)) {
            stream.filter(Files::isRegularFile).forEach(f -> {
                try {
                    String fileName = f.getFileName() != null ? f.getFileName().toString() : UNKNOWN_FILE_NAME;
                    String uploadPath = msg.getSourceSystem() + "/" +  msg.getBatchId() + "/" + msg.getUniqueConsumerRef() + "/" + PRINT_FOLDER_NAME + "/" + fileName;

                    String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                    printFiles.add(new PrintFile(blob));

                    logger.info("📤 Uploaded print file: {} -> {}", fileName, blob);
                } catch (Exception e) {
                    logger.warn("⚠️ Failed to upload print file: {}", f, e);
                }
            });
        } catch (IOException e) {
            logger.error("❌ Failed to list files in '{}' directory: {}", PRINT_FOLDER_NAME, printDir, e);
        }

        return printFiles;
    }
