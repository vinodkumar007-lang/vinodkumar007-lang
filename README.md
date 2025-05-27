public class ApiResponse {
    private String message;
    private String status;
    private SummaryPayload summaryPayload;

    // Getters and Setters
}
public class PrintFile {
    private String printFileURL;

    // Getters and Setters
}
public class SummaryProcessedFile {
    private String customerID;
    private String accountNumber;
    private String pdfArchiveFileURL;
    private String pdfEmailFileURL;
    private String htmlEmailFileURL;
    private String txtEmailFileURL;
    private String pdfMobstatFileURL;
    private String statusCode;
    private String statusDescription;

    // Getters and Setters
}
public class Payload {
    private String uniqueConsumerRef;
    private String uniqueECPBatchRef;
    private String runPriority;
    private String eventID;
    private String eventType;
    private String restartKey;

    // Getters and Setters
}
public class Metadata {
    private int totalFilesProcessed;
    private String processingStatus;
    private String eventOutcomeCode;
    private String eventOutcomeDescription;

    // Getters and Setters
}
public class Header {
    private String tenantCode;
    private String channelID;
    private String audienceID;
    private String timestamp;
    private String sourceSystem;
    private String product;
    private String jobName;

    // Getters and Setters
}
public class SummaryPayload {
    private String batchID;
    private String fileName;
    private Header header;
    private Metadata metadata;
    private Payload payload;
    private List<SummaryProcessedFile> processedFiles;
    private List<PrintFile> printFiles;
    private String summaryFileURL;
    private String timestamp;

    // Getters and Setters
}
public class BatchFile {
    private String ObjectId;
    private String RepositoryId;
    private String BlobUrl;
    private String Filename;
    private String ValidationStatus;

    // Getters and Setters
}
public class KafkaMessage {
    private String BatchId;
    private String SourceSystem;
    private String TenantCode;
    private String ChannelID;
    private String AudienceID;
    private String Product;
    private String JobName;
    private String UniqueConsumerRef;
    private Double Timestamp;
    private String RunPriority;
    private String EventType;
    private List<BatchFile> BatchFiles;

    // Getters and Setters
}
