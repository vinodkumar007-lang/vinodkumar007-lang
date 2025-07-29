 long dataCount = batchFiles.stream()
                    .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                    .count();
            long refCount = batchFiles.stream()
                    .filter(f -> "REF".equalsIgnoreCase(f.getFileType()))
                    .count();

            // 1. DATA only ✅
            if (dataCount == 1 && refCount == 0) {
                logger.info("✅ Valid batch {} with 1 DATA file", batchId);
            }
            // 2. Multiple DATA ❌
            else if (dataCount > 1) {
                logger.error("❌ Rejected batch {} - Multiple DATA files", batchId);
                ack.acknowledge();
                return;
            }
            // 3. REF only ❌
            else if (dataCount == 0 && refCount > 0) {
                logger.error("❌ Rejected batch {} - Only REF files", batchId);
                ack.acknowledge();
                return;
            }
            // 4. REF + DATA ✅ (but ignore REF)
            else if (dataCount == 1 && refCount > 0) {
                logger.info("✅ Valid batch {} with DATA + REF (REF will be ignored)", batchId);
                message.setBatchFiles(
                        batchFiles.stream()
                                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                                .toList()
                );
            }
            // 5. Unknown or empty file types ❌
            else {
                logger.error("❌ Rejected batch {} - Invalid or unsupported file type combination", batchId);
                ack.acknowledge();
                return;
            }
