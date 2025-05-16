private String convertPojoToJson(String raw) {
    // Step 1: Remove the class wrapper (e.g., "PublishEvent(...)" -> "...")
    raw = raw.trim();
    if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
        raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
    }

    // Step 2: Replace nested class names like BatchFile(...) with just {...}
    raw = raw.replaceAll("\\b\\w+\\(", "{");
    raw = raw.replaceAll("\\)", "}"); // Close any open braces

    // Step 3: Replace key=value with "key":"value" (add quotes around values if needed)
    raw = raw.replaceAll("(\\w+)=(\\{.*?\\}|\"[^\"]*\"|[^,{}\\[\\]]+)", "\"$1\":$2");

    // Step 4: Quote remaining object keys
    raw = raw.replaceAll("([,\\{\\[])(\\s*)(\\w+)(\\s*):", "$1$2\"$3\"$4:");

    // Step 5: Remove trailing commas (which are invalid in JSON)
    raw = raw.replaceAll(",\\s*([}\\]])", "$1");

    // Step 6: Wrap in {} if needed
    raw = raw.trim();
    if (!raw.startsWith("{")) {
        raw = "{" + raw;
    }
    if (!raw.endsWith("}")) {
        raw += "}";
    }

    return raw;
}
