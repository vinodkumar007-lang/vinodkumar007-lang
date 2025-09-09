/**
 * Extracts the account number from a given filename.
 * Matches account numbers only if separated by underscores, dashes, or at start/end.
 *
 * Examples:
 *   123_statement.pdf         -> 123
 *   abc_123_statement.pdf     -> 123
 *   statement-123-final.pdf   -> 123
 *   statement123.pdf          -> 123
 *   report_v2025.pdf          -> null  (ignored, not an account number)
 *
 * @param fileName the file name to extract from
 * @return the extracted account number, or null if not found
 */
private String extractAccountFromFileName(String fileName) {
    if (fileName == null || fileName.isBlank()) {
        return null;
    }

    // Remove extension first
    String baseName = fileName.contains(".")
            ? fileName.substring(0, fileName.lastIndexOf('.'))
            : fileName;

    // Regex: account number = 3+ digits, bounded by start, end, underscore, or dash
    Pattern accountPattern = Pattern.compile("(?<=^|_|-)(\\d{3,})(?=$|_|-)", Pattern.CASE_INSENSITIVE);
    Matcher matcher = accountPattern.matcher(baseName);

    if (matcher.find()) {
        return matcher.group(1);
    }

    return null; // no account number found
}
