int customerCount = msg.getBatchFiles().stream()
        .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
        .mapToInt(BatchFile::getCustomerCount)
        .sum();
