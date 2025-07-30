private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
    List<PrintFile> printFiles = new ArrayList<>();

    if (jobDir == null || msg == null || msg.getSourceSystem() == null) {
        logger.error("❌ Invalid input: jobDir={}, msg={}, sourceSystem={}", jobDir, msg, msg != null ? msg.getSourceSystem() : null);
        return printFiles;
    }

    Path printDir = jobDir.resolve("print");
    if (!Files.exists(printDir)) {
        logger.info("ℹ️ No 'print' directory found in jobDir: {}", jobDir);
        return printFiles;
    }

    try (Stream<Path> stream = Files.list(printDir)) {
        stream.filter(Files::isRegularFile).forEach(f -> {
            try {
                String fileName = f.getFileName() != null ? f.getFileName().toString() : "unknown_file";
                String uploadPath = msg.getSourceSystem() + "/print/" + fileName;

                String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                printFiles.add(new PrintFile(blob));

                logger.info("📤 Uploaded print file: {} -> {}", fileName, blob);
            } catch (Exception e) {
                logger.warn("⚠️ Failed to upload print file: {}", f, e);
            }
        });
    } catch (IOException e) {
        logger.error("❌ Failed to list files in print directory: {}", printDir, e);
    }

    return printFiles;
}
