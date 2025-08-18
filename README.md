Path jobDir = Paths.get(mountPath, AppConstants.OUTPUT_FOLDER, message.getSourceSystem(), otResponse.getJobId());
            logger.info("[{}] 🔄 Invoking buildDetailedProcessedFiles...", batchId);
