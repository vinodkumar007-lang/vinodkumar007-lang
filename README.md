public static String extractAccountFromFileName(String fileName) {
    if (fileName == null) return null;

    // Split filename into parts by underscore
    String[] parts = fileName.split("_");

    for (String part : parts) {
        // ✅ Pick the first numeric token (8–12 digits typical for accounts)
        if (part.matches("\\d{5,12}")) {
            return part;
        }
    }

    return null; // no valid account number found
}
