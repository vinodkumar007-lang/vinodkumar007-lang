private String convertPojoToJson(String raw) {
    // Remove class wrapper
    raw = raw.trim();
    if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
        raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
    }

    // Replace '=' with ':' and quote keys and string values
    raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
    raw = raw.replaceAll(":([a-zA-Z0-9_]+)", ":\"$1\"");

    // Handle nested objects and arrays
    raw = raw.replaceAll("\\{([^}]+)\\}", "\"$1\"");
    raw = raw.replaceAll("\\[([^\\]]+)\\]", "\"$1\"");

    // Escape special characters
    raw = raw.replaceAll("([{}\\[\\]=,])", "\\\\$1");

    // Return as valid JSON
    return "{" + raw + "}";
}
