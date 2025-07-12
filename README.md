Metadata metadata = new Metadata();
metadata.setTotalFilesProcessed(message.getCustomersProcessed() != null ? message.getCustomersProcessed() : 0); // ← updated
metadata.setProcessingStatus("Completed");
metadata.setEventOutcomeCode("0");
metadata.setEventOutcomeDescription("Success");
payload.setMetadata(metadata);
