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

    boolean isEmailNA = "NA".equals(email);
    boolean isPrintNA = "NA".equals(print);
    boolean isMobstatNA = "NA".equals(mobstat);

    // ✅ SUCCESS cases: one delivery + archive + others NA
    if (isEmailSuccess && isArchiveSuccess && isPrintNA && isMobstatNA) {
        entry.setOverallStatus("SUCCESS");
    } else if (isMobstatSuccess && isArchiveSuccess && isEmailNA && isPrintNA) {
        entry.setOverallStatus("SUCCESS");
    } else if (isPrintSuccess && isArchiveSuccess && isEmailNA && isMobstatNA) {
        entry.setOverallStatus("SUCCESS");
    }

    // ✅ PARTIAL if archive success but others missing or failed (not NA)
    else if (isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed && isPrintMissingOrFailed) {
        entry.setOverallStatus("PARTIAL");
    } else if (isArchiveSuccess) {
        entry.setOverallStatus("PARTIAL");
    }

    // ❌ All failed
    else {
        entry.setOverallStatus("FAILED");
    }
}
