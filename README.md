public PublishEvent parseToPublishEvent(String raw) {
    try {
        // Step 1: Strip the outer PublishEvent(...)
        if (raw.startsWith("PublishEvent(")) {
            raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
        }

        // Step 2: Replace BatchFile(...) with proper JSON object
        raw = raw.replaceAll("BatchFile\\(", "\\{").replaceAll("\\)", "}");

        // Step 3: Quote objectId content inside {}
        raw = raw.replaceAll("objectId=\\{([^}]+)}", "objectId:\"$1\"");

        // Step 4: Quote keys and values properly (use non-greedy match for values)
        raw = raw.replaceAll("(\\w+)=([^,\\[{]+)", "\"$1\":\"$2\"");

        // Step 5: Fix batchFiles array brackets
        raw = raw.replace("\"batchFiles\":\"[", "\"batchFiles\":[");
        raw = raw.replace("]\"", "]");

        // Step 6: Add JSON braces
        String json = "{" + raw + "}";

        // Step 7: Use Jackson ObjectMapper to parse
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules(); // for ZonedDateTime, etc.
        return mapper.readValue(json, PublishEvent.class);

    } catch (Exception e) {
        throw new RuntimeException("Failed to parse to PublishEvent", e);
    }
}
