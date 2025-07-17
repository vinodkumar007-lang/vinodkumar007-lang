public class CustomerSummary {
    private String customerId;
    private List<AccountSummary> accounts = new ArrayList<>();

    // getters/setters
}

public class AccountSummary {
    private String accountNumber;
    private String statusCode;
    private String statusDescription;
    private String pdfEmailStatus;
    private String pdfArchiveStatus;
    private String pdfMobstatStatus;
    private String printStatus;

    // getters/setters
}

private static List<CustomerSummary> buildCustomerSummaries(List<SummaryProcessedFile> processedFiles) {
    Map<String, CustomerSummary> customerMap = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String customerId = file.getCustomerId();
        String account = file.getAccountNumber();
        if (customerId == null || account == null) continue;

        CustomerSummary customerSummary = customerMap.computeIfAbsent(customerId, id -> {
            CustomerSummary cs = new CustomerSummary();
            cs.setCustomerId(id);
            return cs;
        });

        AccountSummary acc = new AccountSummary();
        acc.setAccountNumber(account);
        acc.setStatusCode(file.getStatusCode());
        acc.setStatusDescription(file.getStatusDescription());
        acc.setPdfEmailStatus(file.getPdfEmailStatus());
        acc.setPdfArchiveStatus(file.getPdfArchiveStatus());
        acc.setPdfMobstatStatus(file.getPdfMobstatStatus());
        acc.setPrintStatus(file.getPrintStatus());

        customerSummary.getAccounts().add(acc);
    }

    return new ArrayList<>(customerMap.values());
}

// Metadata block
Metadata metadata = new Metadata();
metadata.setTotalFilesProcessed(customersProcessed);
metadata.setProcessingStatus(overallStatus);
metadata.setEventOutcomeCode("0");
metadata.setEventOutcomeDescription("Success");

// ⬇️ Add this line
metadata.setCustomerSummaries(buildCustomerSummaries(processedFiles));

payload.setMetadata(metadata);

private List<CustomerSummary> customerSummaries = new ArrayList<>();
