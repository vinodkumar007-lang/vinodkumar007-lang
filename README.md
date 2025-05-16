private String convertPojoToJson(String raw) {
    // Remove root class wrapper e.g. "{PublishEvent(...)}" -> "..."
    raw = raw.trim();
    if (raw.startsWith("{") && raw.contains("(")) {
        int firstParen = raw.indexOf('(');
        int lastParen = raw.lastIndexOf(')');
        if (firstParen > 0 && lastParen > firstParen) {
            raw = raw.substring(firstParen + 1, lastParen);
        }
    }

    // Replace "=" with ":" and quote keys and string values
    raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
    raw = raw.replaceAll(":([a-zA-Z/_\\-.@]+)", ":\"$1\"");

    // Quote timestamps and unquoted string values
    raw = raw.replaceAll(":\"(\\d{4}-\\d{2}-\\d{2}T[^\"]+)\"", ":\"$1\"");
    raw = raw.replaceAll(":([a-zA-Z_][a-zA-Z0-9_]+)", ":\"$1\"");

    // Fix nested objectId and similar internal braces
    raw = raw.replaceAll("objectId=\\{([^}]+)}", "\"objectId\": \"$1\"");
    raw = raw.replaceAll("(?<=[\\[,\\{])([a-zA-Z0-9_]+)(?==)", "\"$1\"");

    // Fix lists
    raw = raw.replace("],", "], ");

    return "{" + raw + "}";
}
