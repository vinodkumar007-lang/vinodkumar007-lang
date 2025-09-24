long totalCustomersProcessed = processedFileEntries.stream()
        .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl()) 
                   || isNonEmpty(pf.getEmailBlobUrlPdf())
                   || isNonEmpty(pf.getEmailBlobUrlHtml())
                   || isNonEmpty(pf.getEmailBlobUrlText())
                   || isNonEmpty(pf.getMobstatBlobUrl()))
        .map(pf -> pf.getCustomerId() + "|" + pf.getAccountNumber())
        .distinct()
        .count();
metadata.setTotalCustomersProcessed((int) totalCustomersProcessed);
