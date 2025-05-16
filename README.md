private String convertPojoToJson(String raw) {
    // Remove outer wrapper like PublishEvent(...)
    if (raw.contains("(") && raw.endsWith(")")) {
        int firstParen = raw.indexOf('(');
        int lastParen = raw.lastIndexOf(')');
        raw = raw.substring(firstParen + 1, lastParen);
    }

    // Replace nested object notations like BatchFile(...) with { ... }
    raw = raw.replaceAll("(\\w+)\\(", "{");
    raw = raw.replaceAll("\\)", "}");

    // Replace objectId={...} with "objectId": "{...}"
    raw = raw.replaceAll("(\\w+)=\\{([^}]+)}", "\"$1\": \"{$2}\"");

    // Quote key names and values properly
    raw = raw.replaceAll("(\\w+)=([^,\\{\\}\\[\\]]+)", "\"$1\": \"$2\"");

    // Replace `,` between fields but not inside nested brackets
    raw = raw.replaceAll(",\\s*(\\w+=)", ", \"$1");

    // Handle arrays manually — batchFiles=[{...}, {...}]
    raw = raw.replaceAll("(\\w+)=\\[", "\"$1\": [");

    // Fix any values left unquoted like `null`, `true`, `12345` (only skip numbers and booleans)
    raw = raw.replaceAll(":\\s*([^\"\\[\\{\\dtruefalsenull][^,\\}\\]]*)", ": \"$1\"");

    // Wrap the final result in curly braces
    return "{" + raw + "}";
}
