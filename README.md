package com.nedbank.kafka.filemanage.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class CustomerSum {

    private String customerId;
    private List<Account> accounts = new ArrayList<>();  // List of accounts per customer
    private Map<String, String> deliveryStatus = new HashMap<>();
    private String status;

    // Optional: safely add an account
    public void addAccount(Account account) {
        if (account != null) {
            accounts.add(account);
        }
    }

    @Override
    public String toString() {
        return "CustomerSum{" +
                "customerId='" + customerId + '\'' +
                ", accounts=" + accounts +
                ", deliveryStatus=" + deliveryStatus +
                ", status='" + status + '\'' +
                '}';
    }
}

package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class Account {
    private String accountNumber;
    private String cisNumber;
    private String status;
}

private List<CustomerSum> buildGroupedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) {

    Map<String, CustomerSum> customerMap = new HashMap<>();

    for (SummaryProcessedFile entry : customerList) {
        String customerId = entry.getCustomerId();
        String account = entry.getAccountNumber();

        CustomerSum customerSummary = customerMap.computeIfAbsent(customerId, k -> {
            CustomerSum cs = new CustomerSum();
            cs.setCustomerId(customerId);
            return cs;
        });

        // Check if this account already exists
        boolean alreadyProcessed = customerSummary.getAccounts().stream()
                .anyMatch(a -> a.getAccountNumber().equals(account));

        if (!alreadyProcessed) {
            Account accountObj = new Account();
            accountObj.setAccountNumber(entry.getAccountNumber());
            accountObj.setCisNumber(entry.getCisNumber());
            accountObj.setStatus(entry.getStatusCode());
            customerSummary.addAccount(accountObj);
        }

        // Add delivery status per channel
        customerSummary.getDeliveryStatus().put(entry.getChannel(), entry.getStatusCode());

        // Optional: pick latest non-success status as overall
        if (!"SUCCESS".equalsIgnoreCase(entry.getStatusCode())) {
            customerSummary.setStatus(entry.getStatusCode());
        } else if (customerSummary.getStatus() == null) {
            customerSummary.setStatus("SUCCESS");
        }
    }

    return new ArrayList<>(customerMap.values());
}

public static SummaryPayload buildPayload(KafkaMessage message,
    List<CustomerSum> customerSummaries,
    int pagesProcessed,
    List<PrintFile> printFiles,
    String mobstatTriggerPath,
    int customersProcessed)


payload.setProcessedFiles(customerSummaries);
