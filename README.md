private String safeGetText(JsonNode node, String fieldName, boolean required) throws IOException {
    JsonNode fieldNode = node.get(fieldName);
    if (fieldNode == null) {
        if (required) {
            throw new IOException("Missing required field: " + fieldName);
        } else {
            return null;
        }
    }
    return fieldNode.asText();
}
