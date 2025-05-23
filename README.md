if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
    logger.warn("Skipping message due to missing or empty BatchFiles: " + message);
    continue; // skip this message instead of throwing an exception
}
