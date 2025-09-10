private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
    List<PrintFile> printFiles = new ArrayList<>();

    if (jobDir == null || msg == null || msg.getSourceSystem() == null) {
        logger.error("❌ Invalid input: jobDir={}, msg={}, sourceSystem={}", 
                jobDir, msg, msg != null ? msg.getSourceSystem() : null);
        return printFiles;
    }

    try (Stream<Path> allDirs = Files.walk(jobDir)) {
        // Step 1: Find all directories whose name contains "print"
        List<Path> printDirs = allDirs
                .filter(Files::isDirectory)
                .filter(p -> p.getFileName().toString().trim().toLowerCase()
                        .contains(AppConstants.PRINT_FOLDER_NAME.toLowerCase()))
                .toList();

        if (printDirs.isEmpty()) {
            logger.info("ℹ️ No '{}' directories found under jobDir: {}", 
                    AppConstants.PRINT_FOLDER_NAME, jobDir);
            return printFiles;
        }

        // Step 2: Process all .ps files under each matching directory
        for (Path printDir : printDirs) {
            try (Stream<Path> files = Files.walk(printDir)) {
                files.filter(Files::isRegularFile)
                        .filter(f -> f.getFileName().toString().toLowerCase().endsWith(".ps"))
                        .forEach(f -> {
                            try {
                                String fileName = f.getFileName() != null 
                                        ? f.getFileName().toString() 
                                        : AppConstants.UNKNOWN_FILE_NAME;

                                String uploadPath = msg.getSourceSystem() + "/" +
                                        msg.getBatchId() + "/" +
                                        msg.getUniqueConsumerRef() + "/" +
                                        AppConstants.PRINT_FOLDER_NAME + "/" + fileName;

                                String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                                printFiles.add(new PrintFile(blob));

                                logger.info("📤 Uploaded print file: {} -> {}", fileName, blob);
                            } catch (Exception e) {
                                logger.warn("⚠️ Failed to upload print file: {}", f, e);
                            }
                        });
            }
        }
    } catch (IOException e) {
        logger.error("❌ Failed to list files in '{}' directories under jobDir: {}", 
                AppConstants.PRINT_FOLDER_NAME, jobDir, e);
    }

    return printFiles;
}
