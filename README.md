// --- Helper for MFC to match by account ---
private String findFileByAccount(Map<String, String> fileMap, String account) {
    if (account == null) return null;
    return fileMap.entrySet().stream()
            .filter(e -> {
                String fileName = e.getKey();
                return fileName.startsWith(account + "_")               // case: 12345_statement.pdf
                        || fileName.contains("_" + account + "_")      // case: statement_12345_extra.pdf
                        || fileName.endsWith("_" + account + ".pdf");  // case: Statement-2025-08-02_12345.pdf (MFC)
            })
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
}
