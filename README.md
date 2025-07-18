// METADATA
Metadata metadata = new Metadata();
metadata.setTotalFilesProcessed((int) processedList.stream()
        .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
        .distinct()
        .count());
metadata.setProcessingStatus("Completed");
metadata.setEventOutcomeCode("0");
metadata.setEventOutcomeDescription("Success");
payload.setMetadata(metadata);
