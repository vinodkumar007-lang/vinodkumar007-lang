public String uploadFile(File file, String folderName, KafkaMessage msg) {
    try {
        byte[] content = Files.readAllBytes(file.toPath());
        String targetPath = buildBlobPath(file.getName(), folderName, msg);
        return uploadFile(content, targetPath);
    } catch (IOException e) {
        logger.error("❌ Error reading file for upload: {}", file.getAbsolutePath(), e);
        throw new CustomAppException("File read failed", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}

private String buildBlobPath(String fileName, String folderName, KafkaMessage msg) {
    return msg.getTenantCode() + "/" +
           msg.getSourceSystem() + "/" +
           msg.getConsumerReference() + "/" +
           msg.getProcessReference() + "/" +
           msg.getTimestamp() + "/" +
           folderName + "/" +
           fileName;
}
