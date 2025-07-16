Map<String, SummaryProcessedFile> customerMap = list.stream()
    .collect(Collectors.toMap(
        SummaryProcessedFile::getAccountNumber,
        Function.identity(),
        (existing, duplicate) -> existing // or keep latest with: duplicate
    ));
