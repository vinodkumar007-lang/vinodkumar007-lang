String allFileNames = batchFiles.stream()
    .map(BatchFile::getFileName)
    .collect(Collectors.joining(", "));
