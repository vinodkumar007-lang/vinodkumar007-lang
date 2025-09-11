 // --- Helper: account-based matching ---
    private String findFileByAccount(Map<String, String> fileMap, String account) {
        if (account == null) return null;
        return fileMap.entrySet().stream()
                .filter(e -> {
                    String fileName = e.getKey();
                    return fileName.startsWith(account + "_")
                            || fileName.contains("_" + account + "_")
                            || fileName.endsWith("_" + account + ".pdf");
                })
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
