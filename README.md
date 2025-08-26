// Count total customers based on archive entries only
long totalArchiveEntries = processedFileEntries.stream()
        .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl()))
        .count();
metadata.setTotalCustomersProcessed((int) totalArchiveEntries);
