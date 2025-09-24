long totalCustomersProcessed = customerList.stream()
        .filter(c -> c != null && c.getAccountNumber() != null)
        .map(c -> c.getCustomerId() + "|" + c.getAccountNumber())
        .distinct()
        .count();
metadata.setTotalCustomersProcessed((int) totalCustomersProcessed);
