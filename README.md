long totalCustomersProcessed = processedFileEntries.stream()
        .flatMap(pf -> Stream.of(
                pf.getArchiveBlobUrl(),
                pf.getEmailBlobUrlPdf(),
                pf.getEmailBlobUrlHtml(),
                pf.getEmailBlobUrlText(),
                pf.getPdfMobstatFileUrl(),
                pf.getPrintFileUrl()
        ).filter(Objects::nonNull))
        .map(url -> { 
            // Map back to customerId|accountNumber for each URL
            return processedFileEntries.stream()
                .filter(pf -> Stream.of(
                        pf.getArchiveBlobUrl(),
                        pf.getEmailBlobUrlPdf(),
                        pf.getEmailBlobUrlHtml(),
                        pf.getEmailBlobUrlText(),
                        pf.getPdfMobstatFileUrl(),
                        pf.getPrintFileUrl()
                ).anyMatch(u -> u != null))
                .map(pf -> pf.getCustomerId() + "|" + pf.getAccountNumber())
                .findFirst()
                .orElse(null);
        })
        .filter(Objects::nonNull)
        .distinct()
        .count();

metadata.setTotalCustomersProcessed((int) totalCustomersProcessed);
