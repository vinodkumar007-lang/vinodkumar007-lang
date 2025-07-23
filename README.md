private static String determineOverallStatus(ProcessedFileEntry entry) {
    String email = safeStatus(entry.getEmailStatus());
    String print = safeStatus(entry.getPrintStatus());
    String mobstat = safeStatus(entry.getMobstatStatus());
    String archive = safeStatus(entry.getArchiveStatus());

    int successCount = 0;
    int failedCount = 0;
    int emptyCount = 0;

    List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);
    for (String status : allStatuses) {
        if ("SUCCESS".equalsIgnoreCase(status)) {
            successCount++;
        } else if ("FAILED".equalsIgnoreCase(status)) {
            failedCount++;
        } else if (status.isEmpty()) {
            emptyCount++;
        }
    }

    if (successCount == 4) return "SUCCESS";
    if (failedCount > 0 && successCount == 0) return "FAILED";
    if (successCount > 0 && (failedCount > 0 || emptyCount > 0)) return "PARTIAL";
    if (emptyCount == 4) return "FAILED";

    return "PARTIAL";
}

private static String safeStatus(String status) {
    return status == null ? "" : status.trim();
}

======

private static String determineOverallStatus(ProcessedFileEntry entry) {
    String email = safeStatus(entry.getEmailStatus());
    String print = safeStatus(entry.getPrintStatus());
    String mobstat = safeStatus(entry.getMobstatStatus());
    String archive = safeStatus(entry.getArchiveStatus());

    int successCount = 0;
    int failedCount = 0;
    int emptyCount = 0;

    List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);
    for (String status : allStatuses) {
        if ("SUCCESS".equalsIgnoreCase(status)) {
            successCount++;
        } else if ("FAILED".equalsIgnoreCase(status)) {
            failedCount++;
        } else if (status.isEmpty()) {
            emptyCount++;
        }
    }

    if (successCount == 4) return "SUCCESS";
    if (failedCount > 0 && successCount == 0) return "FAILED";
    if (successCount > 0 && (failedCount > 0 || emptyCount > 0)) return "PARTIAL";
    if (emptyCount == 4) return "FAILED";

    return "PARTIAL";
}

private static String safeStatus(String status) {
    return status == null ? "" : status.trim();
}
