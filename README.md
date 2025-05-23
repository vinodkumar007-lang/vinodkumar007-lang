private String extractField(JsonNode root, String fieldName) {
    // Try camelCase first
    JsonNode fieldNode = root.get(fieldName);
    if (fieldNode == null) {
        // Try PascalCase variant
        String altFieldName = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        fieldNode = root.get(altFieldName);
    }
    return fieldNode != null ? fieldNode.asText() : null;
}

private String safeGetText(JsonNode node, String fieldName, boolean required) throws IOException {
    JsonNode fieldNode = node.get(fieldName);
    if (fieldNode == null) {
        // Try PascalCase variant
        String altFieldName = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
        fieldNode = node.get(altFieldName);
    }
    if (fieldNode == null) {
        if (required) {
            throw new IOException("Missing required field: " + fieldName);
        } else {
            return null;
        }
    }
    return fieldNode.asText();
}

private HeaderInfo buildHeader(JsonNode root, String jobName) {
    HeaderInfo headerInfo = new HeaderInfo();
    headerInfo.setBatchId(extractField(root, "batchId"));
    headerInfo.setTenantCode(extractField(root, "tenantCode"));
    headerInfo.setChannelID(extractField(root, "channelID"));
    headerInfo.setAudienceID(extractField(root, "audienceID"));
    headerInfo.setSourceSystem(extractField(root, "sourceSystem"));
    headerInfo.setProduct(extractField(root, "product"));
    headerInfo.setJobName(jobName);

    // Handle timestamp: check both camelCase and PascalCase keys, convert epoch to date string
    JsonNode tsNode = root.get("timestamp");
    if (tsNode == null) {
        tsNode = root.get("Timestamp");
    }
    if (tsNode != null) {
        if (tsNode.isNumber()) {
            long epochSeconds = tsNode.asLong();
            Date date = new Date(epochSeconds * 1000L);
            headerInfo.setTimestamp(date.toString());
        } else {
            headerInfo.setTimestamp(tsNode.asText());
        }
    } else {
        headerInfo.setTimestamp(new Date().toString());
    }

    return headerInfo;
}
