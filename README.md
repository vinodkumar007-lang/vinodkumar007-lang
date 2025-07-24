// ✅ Clean up output: Replace NOT_FOUND with blank string
for (ProcessedFileEntry entry : grouped.values()) {
    if ("NOT_FOUND".equals(entry.getEmailStatus())) entry.setEmailStatus("");
    if ("NOT_FOUND".equals(entry.getPrintStatus())) entry.setPrintStatus("");
    if ("NOT_FOUND".equals(entry.getMobstatStatus())) entry.setMobstatStatus("");
    if ("NOT_FOUND".equals(entry.getArchiveStatus())) entry.setArchiveStatus("");
}
