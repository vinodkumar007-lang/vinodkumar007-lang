public static String extractAccountFromFileName(String fileName) {
    if (fileName == null) return null;

    // Regex: first sequence of digits either before or after underscore
    Matcher matcher = Pattern.compile("(?:^|_)(\\d+)").matcher(fileName);
    if (matcher.find()) {
        return matcher.group(1); // first numeric sequence found
    }

    logger.warn("⚠️ Could not extract account from filename: {}", fileName);
    return null;
}
