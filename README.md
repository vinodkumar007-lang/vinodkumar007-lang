private String findFileByAccount(Map<String, String> fileMap, String account) {
        if (account == null) return null;
        return fileMap.entrySet().stream()
                .filter(e -> e.getKey().contains(account))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
