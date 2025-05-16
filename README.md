private String convertPojoToJson(String raw) {
    try {
        ToStringJsonParser parser = new ToStringJsonParser();
        return parser.parse(raw);
    } catch (Exception ex) {
        logger.error("Failed to parse using ToStringJsonParser. Falling back to regex method: {}", ex.getMessage(), ex);
    }

    // Fallback logic if parser fails
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

    // Fix objectId and nested object handling
    raw = raw.replaceAll("objectId=\\{([^}]+)}", "\"objectId\": \"$1\"");

    // Fix unquoted keys after brackets
    raw = raw.replaceAll("(?<=[\\[,\\{])([a-zA-Z0-9_]+)(?==)", "\"$1\"");

    // Fix lists
    raw = raw.replace("],", "], ");

    return "{" + raw + "}";
}

<dependency>
    <groupId>com.github.foxsamuel</groupId>
    <artifactId>to-string-json-parser</artifactId>
    <version>1.0.0</version>
</dependency>
