// Total unique customers processed
int totalUniqueCustomers = (int) processedFileEntries.stream()
        .map(ProcessedFileEntry::getAccountNumber) // or getCustomerId()
        .filter(Objects::nonNull)
        .distinct()
        .count();
metadata.setTotalCustomersProcessed(totalUniqueCustomers);

// Total unique file URLs (email, print, mobstat, archive)
int totalUniqueFiles = (int) processedFileEntries.stream()
        .flatMap(entry -> Stream.of(
                entry.getEmailBlobUrl(),
                entry.getPrintBlobUrl(),
                entry.getMobstatBlobUrl(),
                entry.getArchiveBlobUrl()
        ))
        .filter(Objects::nonNull)
        .distinct()
        .count();
payload.getPayload().setFileCount(totalUniqueFiles);
