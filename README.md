private String buildTargetBlobPath(String sourceSystem, Double timestamp, String batchId,
                                   String consumerRef, String processRef, String fileName) {
    String upperSourceSystem = sourceSystem != null ? sourceSystem.toUpperCase() : "UNKNOWN";
    return String.format("%s/input/%s", upperSourceSystem, fileName);
}
