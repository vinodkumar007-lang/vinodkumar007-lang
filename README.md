// Check if this account already processed inside customer's accounts
            boolean alreadyProcessed = customerSummary.getAccounts().stream()
                    .anyMatch(a -> a.getAccountNumber().equals(account));
