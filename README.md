int totalUniqueFiles = (int) processedList.stream()
        .flatMap(entry -> Stream.of(
                entry.getEmailBlobUrlPdf(),
                entry.getEmailBlobUrlHtml(),
                entry.getEmailBlobUrlText(),
                entry.getPdfMobstatFileUrl(),
                entry.getArchiveBlobUrl()
        ))
        .filter(url -> url != null && !url.isBlank()) // ✅ only include actually available files
        .distinct()
        .count();
