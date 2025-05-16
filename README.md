public PublishEvent parseToPublishEvent(String raw) {
    try {
        // Extract the inner content from PublishEvent(...)
        raw = raw.trim();
        if (raw.startsWith("PublishEvent(")) {
            raw = raw.substring("PublishEvent(".length(), raw.length() - 1);
        }

        // Remove BatchFile(...) wrapper from inside batchFiles array
        raw = raw.replace("BatchFile(", "{").replace(")", "}");

        // Replace objectId={...} with objectId:"..."
        raw = raw.replaceAll("objectId=\\{([^}]+)}", "objectId:\"$1\"");

        // Quote all key=value pairs
        raw = raw.replaceAll("(\\w+)=([^,\\[]+)(?=,|$)", "\"$1\":\"$2\"");

        // Fix timestamp value that has timezone in square brackets
        raw = raw.replaceAll("\"timestamp\":\"([^\"]+)\\[(.*?)\\]\"", "\"timestamp\":\"$1[$2]\"");

        // Wrap batchFiles list correctly
        raw = raw.replace("batchFiles=[", "\"batchFiles\":[");
        raw = raw.replace("],", "],");

        // Finalize JSON
        String json = "{" + raw + "}";

        // Use ObjectMapper to convert JSON string to POJO
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules(); // for ZonedDateTime
        return mapper.readValue(json, PublishEvent.class);

    } catch (Exception e) {
        throw new RuntimeException("Failed to parse to PublishEvent", e);
    }
}
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
mapper.findAndRegisterModules();
