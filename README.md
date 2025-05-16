private String convertPojoToJson(String raw) {
    // Remove the class wrapper
    raw = raw.trim();
    if (raw.startsWith("PublishEvent(") && raw.endsWith(")")) {
        raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
    }

    // Replace '=' with ':' and quote keys and string values
    raw = raw.replaceAll("([a-zA-Z0-9_]+)=", "\"$1\":");
    raw = raw.replaceAll(":([a-zA-Z0-9_]+)", ":\"$1\"");

    // Quote string values
    raw = raw.replaceAll("([a-zA-Z0-9_]+)", "\"$1\"");

    // Fix nested objectId and similar internal braces
    raw = raw.replaceAll("objectId=\\{([^}]+)}", "\"objectId\": \"$1\"");

    // Fix lists
    raw = raw.replace("],", "], ");

    return "{" + raw + "}";
}
