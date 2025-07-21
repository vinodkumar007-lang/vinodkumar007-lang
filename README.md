import lombok.Data;

@Data
public class ProcessedFileEntry {
    private String customerId;
    private String accountNumber;
    private String outputMethod;       // e.g., EMAIL, MOBSTAT, PRINT

    private String outputBlobUrl;      // URL for EMAIL/MOBSTAT/PRINT file
    private String outputStatus;       // SUCCESS / FAILED / NOT-FOUND

    private String archiveBlobUrl;     // Always expected to be present if found
    private String archiveStatus;      // SUCCESS / FAILED / NOT-FOUND

    private String overallStatus;      // SUCCESS / PARTIAL / FAILED
}
