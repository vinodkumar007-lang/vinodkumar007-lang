for (ProcessedFileEntry entry : grouped.values()) {
    String email = entry.getEmailStatus();
    String print = entry.getPrintStatus();
    String mobstat = entry.getMobstatStatus();
    String archive = entry.getArchiveStatus();

    boolean isEmailSuccess = "SUCCESS".equals(email);
    boolean isPrintSuccess = "SUCCESS".equals(print);
    boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
    boolean isArchiveSuccess = "SUCCESS".equals(archive);

    boolean isEmailNA = "NA".equals(email);
    boolean isPrintNA = "NA".equals(print);
    boolean isMobstatNA = "NA".equals(mobstat);

    boolean isEmailFailed = "FAILED".equals(email) || "NOT_FOUND".equals(email);
    boolean isPrintFailed = "FAILED".equals(print) || "NOT_FOUND".equals(print);
    boolean isMobstatFailed = "FAILED".equals(mobstat) || "NOT_FOUND".equals(mobstat);

    // ✅ CASE 1: SUCCESS when one delivery + archive are SUCCESS and rest NA
    if (isEmailSuccess && isArchiveSuccess && isPrintNA && isMobstatNA) {
        entry.setOverallStatus("SUCCESS");
    } else if (isMobstatSuccess && isArchiveSuccess && isEmailNA && isPrintNA) {
        entry.setOverallStatus("SUCCESS");
    } else if (isPrintSuccess && isArchiveSuccess && isEmailNA && isMobstatNA) {
        entry.setOverallStatus("SUCCESS");

    // ✅ CASE 2: PARTIAL if requested method failed (NOT_FOUND/FAILED) + archive SUCCESS
    } else if ((isEmailFailed || isPrintFailed || isMobstatFailed) && isArchiveSuccess) {
        entry.setOverallStatus("PARTIAL");

    // ❌ CASE 3: All FAILED or archive missing
    } else {
        entry.setOverallStatus("FAILED");
    }
}
