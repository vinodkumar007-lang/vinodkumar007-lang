for (ProcessedFileEntry entry : grouped.values()) {
    String email = entry.getEmailStatus();
    String print = entry.getPrintStatus();
    String mobstat = entry.getMobstatStatus();
    String archive = entry.getArchiveStatus();

    boolean isEmailSuccess = "SUCCESS".equals(email);
    boolean isPrintSuccess = "SUCCESS".equals(print);
    boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
    boolean isArchiveSuccess = "SUCCESS".equals(archive);

    boolean isEmailMissingOrFailed = email == null || "FAILED".equals(email) || "NOT_FOUND".equals(email);
    boolean isPrintMissingOrFailed = print == null || "FAILED".equals(print) || "NOT_FOUND".equals(print);
    boolean isMobstatMissingOrFailed = mobstat == null || "FAILED".equals(mobstat) || "NOT_FOUND".equals(mobstat);

    if (isEmailSuccess && isArchiveSuccess) {
        entry.setOverallStatus("SUCCESS");
    } else if (isMobstatSuccess && isArchiveSuccess && isEmailMissingOrFailed && isPrintMissingOrFailed) {
        entry.setOverallStatus("SUCCESS");
    } else if (isPrintSuccess && isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed) {
        entry.setOverallStatus("SUCCESS");
    } else if (isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed && isPrintMissingOrFailed) {
        entry.setOverallStatus("PARTIAL");
    } else if (isArchiveSuccess) {
        entry.setOverallStatus("PARTIAL");
    } else {
        entry.setOverallStatus("FAILED");
    }
}
