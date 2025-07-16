@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class CustomerSummary {
    private String customerId;
    private String accountNumber;
    private List<FileDetail> files;

    // ✅ Add these three new fields:
    private List<String> outputMethods;
    private String cisNumber; // Optional: if you want separate field
    private String status;

    private String pdfArchiveFileURL;
    private String pdfEmailFileURL;
    private String htmlEmailFileURL;
    private String txtEmailFileURL;
    private String pdfMobstatFileURL;
    private String statusCode;
    private String statusDescription;
    private String printFileURL;

    // ... FileDetail inner class remains unchanged ...
}
