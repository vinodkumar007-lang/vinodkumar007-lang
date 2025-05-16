import java.time.ZonedDateTime;
import java.util.List;

public class PublishEvent {
    private String sourceSystem;
    private ZonedDateTime timestamp;
    private List<BatchFile> batchFiles;
    private String consumerReference;
    private String processReference;
    private String batchControlFileData;

    // Getters and setters
}

public class BatchFile {
    private String objectId;
    private String repositoryId;
    private String fileLocation;
    private String validationStatus;

    // Getters and setters
}

public PublishEvent parseToPublishEvent(String raw) {
    raw = raw.replace("PublishEvent(", "").replaceAll("\\)$", "").trim();
    raw = raw.replace("BatchFile(", "").replaceAll("\\)", "");

    // Replace objectId's curly braces
    raw = raw.replaceAll("objectId=\\{([^}]+)}", "objectId=$1");

    // Replace key=value with "key":"value"
    raw = raw.replaceAll("(\\w+)=([^,\\[\\]]+)", "\"$1\":\"$2\"");

    // Convert to JSON-like string
    raw = raw.replace("batchFiles=[", "\"batchFiles\":[{").replace("]", "}]");

    String jsonLike = "{" + raw + "}";

    try {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules(); // for ZonedDateTime
        return mapper.readValue(jsonLike, PublishEvent.class);
    } catch (Exception e) {
        throw new RuntimeException("Failed to parse to PublishEvent", e);
    }
}

try {
    root = objectMapper.readTree(message);
} catch (Exception e) {
    logger.warn("Failed to parse JSON, attempting POJO conversion");

    PublishEvent event = parseToPublishEvent(message);
    // Manually map `event` into a JsonNode if needed:
    root = objectMapper.valueToTree(event);
}
