private String extractAccountFromFileName(String fileName) {
    if (fileName == null) return null;

    // Match any 10–12 digit number anywhere in the filename
    java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("\\d{10,12}").matcher(fileName);
    if (matcher.find()) {
        return matcher.group(); // return the first matching account number
    }
    return null;
}
