 // --- Helper to extract account from filename ---
    public static String extractAccountFromFileName(String fileName) {
        if (fileName == null) return null;

        // pick first sequence of 6–15 digits anywhere in filename
        Matcher matcher = Pattern.compile("\\d{6,20}").matcher(fileName);
        if (matcher.find()) return matcher.group();
        return null;
    }
