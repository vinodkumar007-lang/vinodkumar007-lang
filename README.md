for (ProcessedFileEntry entry : grouped.values()) {
    String email = entry.getEmailStatus();
    String print = entry.getPrintStatus();
    String mobstat = entry.getMobstatStatus();
    String archive = entry.getArchiveStatus();

    // ✅ Fix NOT_FOUND → NA for unused channels
    if ("SUCCESS".equals(email) && "SUCCESS".equals(archive)) {
        if ("NOT_FOUND".equals(mobstat) || mobstat == null) entry.setMobstatStatus("NA");
        if ("NOT_FOUND".equals(print) || print == null) entry.setPrintStatus("NA");
    } else if ("SUCCESS".equals(mobstat) && "SUCCESS".equals(archive)) {
        if ("NOT_FOUND".equals(email) || email == null) entry.setEmailStatus("NA");
        if ("NOT_FOUND".equals(print) || print == null) entry.setPrintStatus("NA");
    } else if ("SUCCESS".equals(print) && "SUCCESS".equals(archive)) {
        if ("NOT_FOUND".equals(email) || email == null) entry.setEmailStatus("NA");
        if ("NOT_FOUND".equals(mobstat) || mobstat == null) entry.setMobstatStatus("NA");
    }

    // ❗ Re-fetch updated values
    email = entry.getEmailStatus();
    print = entry.getPrintStatus();
    mobstat = entry.getMobstatStatus();
    archive = entry.getArchiveStatus();

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
