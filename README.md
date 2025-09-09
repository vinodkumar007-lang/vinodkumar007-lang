private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
    List<PrintFile> printFiles = new ArrayList<>();

    if (jobDir == null || msg == null || msg.getSourceSystem() == null) {
        logger.error("❌ Invalid input: jobDir={}, msg={}, sourceSystem={}", jobDir, msg, msg != null ? msg.getSourceSystem() : null);
        return printFiles;
    }

    if (!Files.exists(jobDir)) {
        logger.warn("ℹ️ Job directory does not exist: {}", jobDir);
        return printFiles;
    }

    try (Stream<Path> stream = Files.walk(jobDir)) {
        stream.filter(Files::isRegularFile)
              .filter(f -> f.getFileName() != null && f.getFileName().toString().toLowerCase().endsWith(".ps"))
              .forEach(f -> {
                  try {
                      String fileName = f.getFileName() != null ? f.getFileName().toString() : AppConstants.UNKNOWN_FILE_NAME;
                      String uploadPath = msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + msg.getUniqueConsumerRef() + "/print/" + fileName;

                      String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                      printFiles.add(new PrintFile(blob));

                      logger.info("📤 Uploaded print file: {} -> {}", fileName, blob);
                  } catch (Exception e) {
                      logger.warn("⚠️ Failed to upload print file: {}", f, e);
                  }
              });
    } catch (IOException e) {
        logger.error("❌ Failed to walk files in jobDir: {}", jobDir, e);
    }

    return printFiles;
}
