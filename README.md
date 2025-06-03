public String extractFileName(String fullPathOrUrl) {
    if (fullPathOrUrl == null || fullPathOrUrl.isEmpty()) {
        return fullPathOrUrl;
    }
    // Remove trailing slashes if any
    String trimmed = fullPathOrUrl.replaceAll("/+$", "");
    // Get the substring after last slash
    int lastSlashIndex = trimmed.lastIndexOf('/');
    if (lastSlashIndex >= 0 && lastSlashIndex < trimmed.length() - 1) {
        return trimmed.substring(lastSlashIndex + 1);
    } else {
        return trimmed; // no slashes found, return as is
    }
}
String sanitizedBlobName = extractFileName(originalBlobPathOrUrl);
String content = blobStorageService.downloadFileContent(sanitizedBlobName);
