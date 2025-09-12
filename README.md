int totalUniqueFiles = (int) processedList.stream()
                .flatMap(entry -> Stream.of(
                        entry.getPdfEmailFileUrl(),
                        entry.getPdfMobstatFileUrl(),
                        entry.getArchiveBlobUrl()
                ))
                .filter(Objects::nonNull)
                .distinct()
                .count();
