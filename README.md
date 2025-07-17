package com.nedbank.kafka.filemanage.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class CustomerSum {

    private List<Account> accounts = new ArrayList<>();
    private String cisNumber;
    private String customerId;
    private Map<String, String> deliveryStatus = new HashMap<>();
    private String status;

    public CustomerSum() {}

    // You can add helper method for easier account checking
    public boolean hasAccount(String accountNumber) {
        return accounts.stream()
            .anyMatch(a -> a.getAccountNumber().equals(accountNumber));
    }
}

package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class Account {
    private String accountNumber;
}
