long totalUniqueCustomers = processedFileEntries.stream()
        .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl()))
        .map(pf -> pf.getCustomerId() + "|" + pf.getAccountNumber()) // unique by customer + account
        .distinct()
        .count();

metadata.setTotalCustomersProcessed((int) totalUniqueCustomers);
