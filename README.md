// --- Helper: extract account from filename (fixed for your archive files) ---
public static String extractAccountFromFileName(String fileName) {
    if (fileName == null) return null;

    // Case 1: account number is prefix before first "_"
    if (fileName.contains("_")) {
        String prefix = fileName.substring(0, fileName.indexOf("_"));
        if (prefix.matches("\\d{8,12}")) { // allow 8–12 digits
            return prefix;
        }
    }

    // Case 2: fallback – first sequence of 8–12 digits anywhere in filename
    Matcher matcher = Pattern.compile("\\d{8,12}").matcher(fileName);
    if (matcher.find()) {
        return matcher.group();
    }

    return null;
}
