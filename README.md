private String buildTargetBlobPath(String sourceSystem, Double timestamp, String batchId,
                                       String consumerRef, String processRef, String fileName) {
        String timestampStr = instantToIsoString(timestamp);
        return String.format("%s/input/%s/%s/%s_%s/%s",
                sourceSystem,
                timestampStr.replace(":", "-"),
                batchId,
                consumerRef,
                processRef,
                fileName);
    }
