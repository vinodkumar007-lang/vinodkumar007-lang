public static String extractAccountFromFileName(String fileName) {
        if (fileName == null || !fileName.contains("_")) return null;
        return fileName.split("_")[0];
    }
