        Set<String> statuses = processedFileEntries.stream()
                .map(ProcessedFileEntry::getOverallStatus)
                .filter(Objects::nonNull)
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        String overallStatus;
        if (statuses.size() == 1) {
            overallStatus = statuses.iterator().next(); // only one unique status
        } else if (statuses.contains("SUCCESS") && statuses.contains("FAILED")) {
            overallStatus = "PARTIAL";
        } else if (statuses.contains("PARTIAL") || statuses.size() > 1) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "FAILED"; // fallback
        }

        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        return payload;
