private String convertPojoToJson(String pojoString) {
    try {
        // Remove any class-like prefixes (e.g., PublishEvent{ => { )
        String json = pojoString.replaceAll("\\w+\\{", "{");

        // Add quotes around keys
        json = json.replaceAll("(\\w+):", "\"$1\":");

        // Add quotes around values that look like strings (words, paths)
        json = json.replaceAll(":([\\w/\\.\\-]+)", ":\"$1\"");

        // Add quotes around values with UUIDs inside curly braces
        json = json.replaceAll(":\\{([A-Fa-f0-9\\-]+)\\}", ":\"{$1}\"");

        // Add quotes around values with timestamps
        json = json.replaceAll(":([\\d\\-T:+\\[\\]A-Za-z]+)", ":\"$1\"");

        // Fix issues where closing brackets are missing
        if (!json.startsWith("{")) json = "{" + json;
        if (!json.endsWith("}")) json = json + "}";

        logger.debug("Converted POJO string to JSON: {}", json);
        return json;
    } catch (Exception ex) {
        logger.error("Failed to convert POJO to JSON: {}", ex.getMessage(), ex);
        return pojoString; // fallback to original
    }
}
