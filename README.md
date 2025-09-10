private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
    List<PrintFile> printFiles = new ArrayList<>();

    if (jobDir == null || msg == null || msg.getSourceSystem() == null) {
        logger.error("❌ Invalid input: jobDir={}, msg={}, sourceSystem={}", jobDir, msg, msg != null ? msg.getSourceSystem() : null);
        return printFiles;
    }

    if (!Files.exists(jobDir)) {
        logger.info("ℹ️ jobDir does not exist: {}", jobDir);
        return printFiles;
    }

    try (Stream<Path> stream = Files.walk(jobDir)) { // recursively walk all files
        stream.filter(Files::isRegularFile)
              .filter(f -> f.getFileName().toString().toLowerCase().endsWith(".ps")) // only .ps files
              .forEach(f -> {
                  try {
                      String fileName = f.getFileName() != null ? f.getFileName().toString() : AppConstants.UNKNOWN_FILE_NAME;

                      // Determine relative path from jobDir for proper folder structure in blob
                      Path relativePath = jobDir.relativize(f.getParent());
                      String uploadPath = msg.getSourceSystem() + "/" + msg.getBatchId() + "/" +
                              msg.getUniqueConsumerRef() + "/" + relativePath.toString().replace("\\", "/") + "/" + fileName;

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
