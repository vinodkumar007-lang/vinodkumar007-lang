 List<SummaryProcessedFile> processedFiles =
                    buildDetailedProcessedFiles(jobDir, customerList, errorMap, message);
            logger.info("[{}] 📦 Processed {} customer records", batchId, processedFiles.size());
