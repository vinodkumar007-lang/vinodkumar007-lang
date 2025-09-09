private List<String> findFilesByAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null || fileMap.isEmpty() || account == null) return Collections.emptyList();

    return fileMap.entrySet().stream()
            .filter(e -> e.getKey().toLowerCase().contains(account.toLowerCase()))
            .map(Map.Entry::getValue)
            .toList();
}
