private String buildBlobPath(String sourceSystem, Double timestamp, String batchId,
                                 String uniqueConsumerRef, String jobName, String folder,
                                 String customerAccountNumber, String fileName) {
        // Fix timestamp parsing here as well
        String datePart = instantToDateString(timestamp);
        return String.format("%s/%s/%s/%s/%s/%s/%s/%s",
                sourceSystem, datePart, batchId, uniqueConsumerRef, jobName, folder, customerAccountNumber, fileName);
    }
