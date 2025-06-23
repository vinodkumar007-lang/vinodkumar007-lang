// === NEW VALIDATION START ===
        List<BatchFile> batchFiles = message.getBatchFiles();

        if (batchFiles == null || batchFiles.isEmpty()) {
            logger.error("BatchFiles is empty or null. Rejecting message.");
            return new ApiResponse("Invalid message: BatchFiles is empty or null", "error",
                    new SummaryPayloadResponse("Invalid message: BatchFiles is empty or null", "error", new SummaryResponse()).getSummaryResponse());
        }

        long dataCount = batchFiles.stream()
                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                .count();

        long refCount = batchFiles.stream()
                .filter(f -> "REF".equalsIgnoreCase(f.getFileType()))
                .count();

        if (dataCount == 0 && refCount > 0) {
            logger.error("Message contains only REF files. Rejecting message.");
            return new ApiResponse("Invalid message: only REF files present", "error",
                    new SummaryPayloadResponse("Invalid message: only REF files present", "error", new SummaryResponse()).getSummaryResponse());
        }

        if (dataCount > 1) {
            logger.error("Message contains multiple DATA files ({}). Rejecting message.", dataCount);
            return new ApiResponse("Invalid message: multiple DATA files present", "error",
                    new SummaryPayloadResponse("Invalid message: multiple DATA files present", "error", new SummaryResponse()).getSummaryResponse());
        }

        // If 1 DATA + REF(s), we process only DATA — so prepare filtered list
        List<BatchFile> validFiles = batchFiles.stream()
                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                .toList();

        logger.info("BatchFiles validation passed. DATA files to process: {}", validFiles.size());
        // === NEW VALIDATION END ===
